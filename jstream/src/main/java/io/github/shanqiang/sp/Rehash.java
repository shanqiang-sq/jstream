package io.github.shanqiang.sp;

import io.github.shanqiang.SystemProperty;
import io.github.shanqiang.Threads;
import io.github.shanqiang.network.Command;
import io.github.shanqiang.network.client.Client;
import io.github.shanqiang.network.server.Server;
import io.github.shanqiang.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.github.shanqiang.sp.QueueSizeLogger.addQueueSizeLog;
import static io.github.shanqiang.table.Table.createEmptyTableLike;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class Rehash {
    private static final Logger logger = LoggerFactory.getLogger(Rehash.class);

    private static final Map<String, Rehash> rehashes = new ConcurrentHashMap<>();
    private final int thread;
    private final String uniqueName;
    private final int myHash;
    private final int serverCount;
    private final String[] hashByColumnNames;
    private final Server server;
    private final Client[][] clients;
    private final Table[][][] tablesInServer;
    private final int batchSize = 40000;
    private final long flushInterval = 1000;
    private final long[] lastFlushTime;
    private final Duration requestTimeout = Duration.ofSeconds(30);
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();
    private final boolean[] finished;

    private static class TableRow {
        private final Table table;
        private final int row;

        private TableRow(Table table, int row) {
            this.table = table;
            this.row = row;
        }
    }

    private final List<BlockingQueue<TableRow>> blockingQueueInThread;

    /**
     * java -jar jstream_task.jar -Dself=localhost:8888 -Dall=localhost:8888,127.0.0.1:9999
     *
     * @param uniqueName must be globally unique. we need use this name to decide rehash data from other servers
     *                   should be processed by which Rehash object.
     *                   automatically generate this name may lead to subtle race condition problem in concurrent case,
     *                   "name order" on different server may be different so let user define this name may be more sensible.
     */
    Rehash(int thread, String uniqueName, String... hashByColumnNames) {
        this.thread = thread;
        this.lastFlushTime = new long[thread];

        blockingQueueInThread = new ArrayList<>(thread);
        for (int i = 0; i < thread; i++) {
            blockingQueueInThread.add(new ArrayBlockingQueue(2 * batchSize));
        }
        addQueueSizeLog(uniqueName, blockingQueueInThread);

        this.uniqueName = requireNonNull(uniqueName);
        this.hashByColumnNames = requireNonNull(hashByColumnNames);
        if (hashByColumnNames.length < 1) {
            throw new IllegalArgumentException();
        }

        this.myHash = SystemProperty.getMyHash();
        this.serverCount = SystemProperty.getServerCount();
        if (null != SystemProperty.getSelf()) {
            Node self = SystemProperty.getSelf();
            server = new Server(false, self.getHost(), self.getPort(), 1, 2);
            startServer();
            clients = new Client[serverCount][thread];
            for (int i = 0; i < serverCount; i++) {
                if (i == myHash) {
                    continue;
                }
                Node node = SystemProperty.getNodeByHash(i);
                for (int j = 0; j < thread; j++) {
                    clients[i][j] = newClient(node.getHost(), node.getPort());
                }
            }
        } else {
            server = null;
            clients = new Client[0][0];
        }
        finished = new boolean[serverCount];
        tablesInServer = new Table[thread][serverCount][thread];

        for (int i = 0; i < thread; i++) {
            tablesInServer[i] = new Table[serverCount][thread];
            for (int j = 0; j < serverCount; j++) {
                tablesInServer[i][j] = new Table[thread];
            }
        }

        rehashes.put(uniqueName, this);
    }

    public void close() {
        if (null != server) {
            server.close();
        }
        for (int i = 0; i < serverCount; i++) {
            if (i == myHash) {
                continue;
            }
            for (int j = 0; j < thread; j++) {
                clients[i][j].close();
            }
        }
        rehashes.remove(uniqueName);
    }

    public void waitOtherServers() {
        if (serverCount <= 1) {
            return;
        }
        finished[myHash] = true;
        for (int i = 0; i < serverCount; i++) {
            if (i == myHash) {
                continue;
            }
            try {
                clients[i][0].request(Command.REHASH_FINISHED, uniqueName, myHash);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        while (true) {
            try {
                reentrantLock.lock();
                boolean flag = true;
                for (int i = 0; i < serverCount; i++) {
                    if (!finished[i]) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    break;
                }
                try {
                    long nanos = condition.awaitNanos(requestTimeout.toNanos());
                    if (nanos <= 0) {
                        throw new RuntimeException("wait timeout");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                reentrantLock.unlock();
            }
        }
    }

    public static int otherServerFinished(String uniqueName, int serverHash) {
        Rehash rehash = rehashes.get(uniqueName);
        rehash.finished(serverHash);
        return 0;
    }

    private void finished(int serverHash) {
        try {
            reentrantLock.lock();
            finished[serverHash] = true;
            condition.signal();
        } finally {
            reentrantLock.unlock();
        }
    }

    private void startServer() {
        newSingleThreadExecutor(Threads.threadsNamed("server")).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    server.start();
                } catch (CertificateException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (SSLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private Client newClient(String host, int port) {
        for (int i = 0; i < 600; i++) {
            try {
                return new Client(false, host, port, requestTimeout);
            } catch (Throwable t) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.info("interrupted");
                }
            }
        }
        throw new RuntimeException(format("cannot create client to host: %s, port: %d", host, port));
    }

    public static int fromOtherServer(String uniqueName, int thread, ByteBuffer data) {
        Rehash rehash = rehashes.get(uniqueName);
        Table table = Table.deserialize(data);
        for (int i = 0; i < table.size(); i++) {
            rehash.blockingQueueInThread.get(thread).add(new TableRow(table, i));
        }
        return table.size();
    }

    private void toAnotherServer(Table table, int row, int hash, int myThreadIndex) {
        int i = hash % serverCount;
        int j = hash % thread;
        if (null == tablesInServer[myThreadIndex][i][j]) {
            tablesInServer[myThreadIndex][i][j] = createEmptyTableLike(table);
        }
        tablesInServer[myThreadIndex][i][j].append(table, row);
        flushBySize(i, j, myThreadIndex);
    }

    private int requestWithRetry(int server, int myThreadIndex, Table table, int toThread) {
        int i = 0;
        int retryTimes = 3;
        while (true) {
            try {
                return clients[server][myThreadIndex].request("rehash",
                        uniqueName,
                        toThread,
                        table);
            } catch (Throwable t) {
                i++;
                logger.error("request error {} times", i, t);
                if (i >= retryTimes) {
                    return -1;
                }
                try {
                    Thread.sleep(5000);
                    Node node = SystemProperty.getNodeByHash(server);
                    clients[server][myThreadIndex].close();
                    clients[server][myThreadIndex] = newClient(node.getHost(), node.getPort());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void request(int server, int toThread, int myThreadIndex) {
        Table table = tablesInServer[myThreadIndex][server][toThread];
        if (null != table && table.size() > 0) {
            int ret = requestWithRetry(server, myThreadIndex, table, toThread);
            if (ret != table.size()) {
                throw new IllegalStateException(format("the peer received size: %d not equal to table.size: %d", ret, table.size()));
            }
            tablesInServer[myThreadIndex][server][toThread] = null;
        }
    }

    private void flushByInterval(int myThreadIndex) {
        long now = System.currentTimeMillis();
        if (now - lastFlushTime[myThreadIndex] >= flushInterval) {
            for (int i = 0; i < serverCount; i++) {
                if (i == myHash) {
                    continue;
                }
                for (int j = 0; j < thread; j++) {
                    request(i, j, myThreadIndex);
                }
            }
            lastFlushTime[myThreadIndex] = now;
        }
    }

    private void flushBySize(int serverHash, int toThread, int myThreadIndex) {
        if (tablesInServer[myThreadIndex][serverHash][toThread].size() >= batchSize) {
            request(serverHash, toThread, myThreadIndex);
        }
    }

    public List<Table> rebalance(Table table, int myThreadIndex) throws InterruptedException {
        return rehash(table, myThreadIndex, false);
    }

    public List<Table> rehash(Table table, int myThreadIndex) throws InterruptedException {
        return rehash(table, myThreadIndex, true);
    }

    private List<Table> rehash(Table table, int myThreadIndex, boolean isHash) throws InterruptedException {
        //长时间没有数据的情况下也可以被触发，参见AbstractStreamTable.consume
        flushByInterval(myThreadIndex);

        Table tmp = createEmptyTableLike(table);
        Random random = null;
        if (!isHash) {
            random = new Random();
        }
        for (int i = 0; i < table.size(); i++) {
            int h;
            if (isHash) {
                List<Comparable> key = new ArrayList<>(hashByColumnNames.length);
                for (int j = 0; j < hashByColumnNames.length; j++) {
                    key.add(table.getColumn(hashByColumnNames[j]).get(i));
                }
                h = abs(key.hashCode());
            } else {
                h = random.nextInt(serverCount * thread);
            }
            if (h % serverCount != myHash) {
                toAnotherServer(table, i, h, myThreadIndex);
                continue;
            }
            h %= thread;
            if (h == myThreadIndex) {
                tmp.append(table, i);
            } else {
                blockingQueueInThread.get(h).put(new TableRow(table, i));
            }
        }

        while (tmp.size() < batchSize) {
            TableRow tableRow = blockingQueueInThread.get(myThreadIndex).poll();
            if (null == tableRow) {
                break;
            }
            tmp.append(tableRow.table, tableRow.row);
        }

        List<Table> ret = new ArrayList<>(1);
        ret.add(tmp);
        return ret;
    }

    List<Table> tablesInThread(int threadIndex) throws InterruptedException {
        List<Table> ret = new ArrayList<>(1);
        BlockingQueue<TableRow> blockingQueue = blockingQueueInThread.get(threadIndex);
        if (blockingQueue.size() <= 0) {
            return ret;
        }

        TableRow tableRow = blockingQueue.poll();
        Table table = createEmptyTableLike(tableRow.table);
        while (null != tableRow) {
            table.append(tableRow.table, tableRow.row);
            tableRow = blockingQueue.poll();
        }

        ret.add(table);
        return ret;
    }
}
