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
import java.util.concurrent.TimeUnit;
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
    private final int targetThread;
    private final String uniqueName;
    private final int myHash;
    private final int serverCount;
    private final String[] hashByColumnNames;
    private final RehashOutputTable[] rehashOutputTables;
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();
    private final boolean[] finished;
    private final boolean rehashBetweenServers;

    static class TableRow {
        final Table table;
        final int row;

        TableRow(Table table, int row) {
            this.table = table;
            this.row = row;
        }
    }

    private final List<BlockingQueue<TableRow>> blockingQueueInThread;

    Rehash(int targetThread, String uniqueName, String... hashByColumnNames) {
        this(targetThread, Runtime.getRuntime().availableProcessors(), 80_0000, true, uniqueName, hashByColumnNames);
    }

    /**
     * java -jar jstream_task.jar -Dself=localhost:8888 -Dall=localhost:8888,127.0.0.1:9999
     *
     * @param uniqueName must be globally unique. we need use this name to decide rehash data from other servers
     *                   should be processed by which Rehash object.
     *                   automatically generate this name may lead to subtle race condition problem in concurrent case,
     *                   "name order" on different server may be different so let user define this name may be more sensible.
     */
    Rehash(int targetThread, int toPerOtherServerThread, int queueSize, boolean rehashBetweenServers, String uniqueName, String... hashByColumnNames) {
        this.targetThread = targetThread;
        this.rehashBetweenServers = rehashBetweenServers;

        blockingQueueInThread = new ArrayList<>(targetThread);
        for (int i = 0; i < targetThread; i++) {
            blockingQueueInThread.add(new ArrayBlockingQueue(queueSize));
        }
        addQueueSizeLog(uniqueName + "-in", blockingQueueInThread);

        this.uniqueName = requireNonNull(uniqueName);
        this.hashByColumnNames = requireNonNull(hashByColumnNames);
        if (hashByColumnNames.length < 1) {
            throw new IllegalArgumentException();
        }

        this.myHash = SystemProperty.getMyHash();
        this.serverCount = SystemProperty.getServerCount();
        finished = new boolean[serverCount];
        rehashOutputTables = new RehashOutputTable[serverCount];
        for (int i = 0; i < serverCount; i++) {
            if (i == myHash) {
                continue;
            }
            rehashOutputTables[i] = new RehashOutputTable(uniqueName, toPerOtherServerThread, i, targetThread, queueSize);
            rehashOutputTables[i].start();
        }

        rehashes.put(uniqueName, this);
    }

    public void close() {
        for (int i = 0; i < serverCount; i++) {
            if (i == myHash) {
                continue;
            }
            rehashOutputTables[i].stop();
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
                rehashOutputTables[i].request(Command.REHASH_FINISHED, uniqueName, myHash);
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
                    Duration requestTimeout = Duration.ofSeconds(30);
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

    public static int fromOtherServer(String uniqueName, int thread, ByteBuffer data) throws InterruptedException {
        Rehash rehash = rehashes.get(uniqueName);
        Table table = Table.deserialize(data);
        for (int i = 0; i < table.size(); i++) {
            rehash.blockingQueueInThread.get(thread).put(new TableRow(table, i));
        }
        return table.size();
    }

    public void rebalance(Table table, int myThreadIndex) throws InterruptedException {
        rehash(table, myThreadIndex, false);
    }

    public void rehash(Table table, int myThreadIndex) throws InterruptedException {
        rehash(table, myThreadIndex, true);
    }

    private void rehash(Table table, int myThreadIndex, boolean isHash) throws InterruptedException {
//        Table tmp = createEmptyTableLike(table);
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
                h = random.nextInt(serverCount * targetThread);
            }
            if (rehashBetweenServers) {
                if (h % serverCount != myHash) {
//                    while (!rehashOutputTables[h % serverCount].produce(table, i, h % targetThread)) {
//                        consumeAll(tmp, myThreadIndex);
//                    }
                    rehashOutputTables[h % serverCount].produce(table, i, h % targetThread);
                    continue;
                }
            }
            h %= targetThread;
//            if (h == myThreadIndex) {
//                tmp.append(table, i);
//            } else {
//                // 自己的队列空对方的队列满的情况下会死循环, timeout 100ms确保CPU不会无谓费电
//                while (!blockingQueueInThread.get(h).offer(new TableRow(table, i), 100, TimeUnit.MILLISECONDS)) {
//                    // offer不到队列里的情况下大家先消费一下自己队列里的数据之后再尝试offer否则互相都offer不进去导致死锁
//                    consumeAll(tmp, myThreadIndex);
//                }
                blockingQueueInThread.get(h).put(new TableRow(table, i));
//            }
        }

//        consumeAll(tmp, myThreadIndex);
//
//        List<Table> ret = new ArrayList<>(1);
//        ret.add(tmp);
//        return ret;
    }

    public Table consume(int myThreadIndex) {
        BlockingQueue<TableRow> blockingQueue = blockingQueueInThread.get(myThreadIndex);
        TableRow tableRow = blockingQueue.poll();
        if (null == tableRow) {
            return null;
        }

        Table table = Table.createEmptyTableLike(tableRow.table);
        while (null != tableRow) {
            // 会不会某个线程快某个线程慢导致慢的那个线程的tmp持续变大？不会。
            // 因为慢的线程可以offer到快的线程里而快的线程offer不到慢的线程里（慢的线程队列更容易满）从而使它们速度拉平
            // 极特殊的情况下比如数据倾斜特别严重的情况下可能出现多个线程往一个线程里持续灌数据的情况导致tmp一直增涨到OOM
            table.append(tableRow.table, tableRow.row);
            tableRow = blockingQueue.poll();
        }
        return table;
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
