package io.github.shanqiang.sp;

import io.github.shanqiang.SystemProperty;
import io.github.shanqiang.Threads;
import io.github.shanqiang.network.Command;
import io.github.shanqiang.network.client.Client;
import io.github.shanqiang.network.server.Server;
import io.github.shanqiang.table.Row;
import io.github.shanqiang.table.RowByTable;
import io.github.shanqiang.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.time.Duration;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.github.shanqiang.sp.QueueSizeLogger.addQueueSizeLog;
import static io.github.shanqiang.table.Table.EMPTY_TABLE;
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
    // 借助Kafka rehash的情况下Kafka里已经rehash到相同的partition里,这种情况下可能只需要在线程间rehash不需要在服务器间再rehash
    private final boolean rehashBetweenServers;
    // 由于是列存, table中存在很大的 VarbyteColumn 的情况下可能很小的table size就产生超过Integer.MAX_VALUE的 VarbyteColumn
    // 因此需要控制 maxTableSize
    private final int maxTableSize;
    // 打开该选项会对疑似哈希不均的key输出log
    // 但该选项很耗性能会使最早队列满的线程持续赶不上其它线程从而造成"假哈希不均"
    // 因此该选项默认为 false 怀疑哈希不均时打开
    private final boolean logHashUneven;
    private long lastIfHashUnevenTime = System.currentTimeMillis();

    static class TableRow {
        final Table table;
        final int row;

        TableRow(Table table, int row) {
            this.table = table;
            this.row = row;
        }
    }

    private final List<BlockingQueue<TableRow>> blockingQueueInThread;

    public Rehash(StreamProcessing target, String uniqueName, String... hashByColumnNames) {
        this(target
                , Runtime.getRuntime().availableProcessors()
                , 100_0000
                , true
                , uniqueName
                , hashByColumnNames);
    }

    public Rehash(StreamProcessing target
            , int toOtherServerBatchSize
            , int queueSize
            , boolean rehashBetweenServers
            , String uniqueName
            , String... hashByColumnNames) {
        this(target, toOtherServerBatchSize, queueSize, queueSize
                , rehashBetweenServers, uniqueName, hashByColumnNames);
    }

    public Rehash(StreamProcessing target
            , int toOtherServerBatchSize
            , int queueSize
            , int maxTableSize
            , boolean rehashBetweenServers
            , String uniqueName
            , String... hashByColumnNames) {
        this(target, toOtherServerBatchSize, queueSize, maxTableSize
                , rehashBetweenServers, false, uniqueName, hashByColumnNames);
    }

    public Rehash(StreamProcessing target
            , int toOtherServerBatchSize
            , int queueSize
            , int maxTableSize
            , boolean rehashBetweenServers
            , boolean logHashUneven
            , String uniqueName
            , String... hashByColumnNames) {
        this(target.thread, toOtherServerBatchSize, queueSize, maxTableSize, rehashBetweenServers, logHashUneven, uniqueName, hashByColumnNames);
    }

    /**
     * java -Dself=localhost:8888 -Dall=localhost:8888,127.0.0.1:9999 -jar jstream_task.jar
     *
     * @param uniqueName must be globally unique. we need use this name to decide rehash data from other servers
     *                   should be processed by which Rehash object.
     *                   automatically generate this name may lead to subtle race condition problem in concurrent case,
     *                   "name order" on different server may be different so let user define this name may be more sensible.
     */
    Rehash(int targetThread, int toOtherServerBatchSize, int queueSize, int maxTableSize, boolean rehashBetweenServers
            , boolean logHashUneven, String uniqueName, String... hashByColumnNames) {
        this.targetThread = targetThread;
        this.rehashBetweenServers = rehashBetweenServers;
        this.maxTableSize = maxTableSize;
        this.logHashUneven = logHashUneven;

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
            rehashOutputTables[i] = new RehashOutputTable(uniqueName, toOtherServerBatchSize, i, targetThread, queueSize);
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
        while (null == rehash) {
            logger.warn("has not created rehash object for uniqueName: {}", uniqueName);
            Thread.sleep(5000);
            // 还没有创建好Rehash对象等5秒重新get直到成功
            rehash = rehashes.get(uniqueName);
        }
        Table table = Table.deserialize(data);
        for (int i = 0; i < table.size(); i++) {
            while (!rehash.blockingQueueInThread.get(thread).offer(new TableRow(table, i), 5, TimeUnit.SECONDS)) {
                logger.warn(format("data from other server exceed 5 seconds cannot offer to %s queue", uniqueName));
            }
        }
        return table.size();
    }

    public void rebalance(Table table) throws InterruptedException {
        rehash(table, false);
    }

    public void rehash(Table table) throws InterruptedException {
        rehash(table, true);
    }

    private void rehash(Table table, boolean isHash) throws InterruptedException {
        Random random = null;
        if (!isHash) {
            random = new Random();
        }
        for (int i = 0; i < table.size(); i++) {
            int h = 0;
            if (isHash) {
                List<Comparable> key = new ArrayList<>(hashByColumnNames.length);
                for (int j = 0; j < hashByColumnNames.length; j++) {
                    key.add(table.getColumn(hashByColumnNames[j]).get(i));
                }
                h = key.hashCode();
            } else {
                h = random.nextInt(serverCount * targetThread);
            }
            // 注意 abs(h) % serverCount 在h == Integer.MAX_VALUE的情况下会由于abs之后溢出产生负值
            int absServer = abs(h % serverCount);
            int absThread = abs(h % targetThread);
            if (rehashBetweenServers) {
                if (absServer != myHash) {
                    rehashOutputTables[absServer].produce(table, i, absThread);
                    continue;
                }
            }
            while (!blockingQueueInThread.get(absThread).offer(new TableRow(table, i), 5, TimeUnit.SECONDS)) {
                logger.warn(format("exceed 5 seconds cannot offer to %s queue", uniqueName));
            }
        }
    }

    private void ifHashUneven(BlockingQueue<TableRow> blockingQueue) {
        if (!logHashUneven) {
            return;
        }

        long now = System.currentTimeMillis();
        if (now - lastIfHashUnevenTime < 5000 || blockingQueue.remainingCapacity() > 0) {
            return;
        }
        synchronized (this) {
            if (now - lastIfHashUnevenTime < 5000) {
                return;
            }
            lastIfHashUnevenTime = now;
        }
        Map<List<Comparable>, AtomicInteger> counter = new HashMap<>();
        for (TableRow tableRow : blockingQueue) {
            Row row = new RowByTable(tableRow.table, tableRow.row);
            List<Comparable> key = new ArrayList<>(hashByColumnNames.length);
            for (int j = 0; j < hashByColumnNames.length; j++) {
                key.add(row.getComparable(hashByColumnNames[j]));
            }
            AtomicInteger atomicInteger = counter.get(key);
            if (null == atomicInteger) {
                atomicInteger = new AtomicInteger(0);
                counter.put(key, atomicInteger);
            }
            atomicInteger.incrementAndGet();
        }

        List<Comparable> maxKey = counter.entrySet().stream().max(
                (entry1, entry2) -> entry1.getValue().get() > entry2.getValue().get() ? 1 : -1
        ).get().getKey();

        logger.warn("may be hash uneven, the max key: {}, the count: {}, total: {}"
                , maxKey, counter.get(maxKey).get(), blockingQueue.size());
    }

    public Table consume(int myThreadIndex) throws InterruptedException {
        return consume(myThreadIndex, 100, TimeUnit.MILLISECONDS);
    }

    public Table consume(int myThreadIndex, long l, TimeUnit timeUnit) throws InterruptedException {
        BlockingQueue<TableRow> blockingQueue = blockingQueueInThread.get(myThreadIndex);
        ifHashUneven(blockingQueue);
        TableRow tableRow = blockingQueue.poll(l, timeUnit);
        if (null == tableRow) {
            return EMPTY_TABLE;
        }

        Table table = Table.createEmptyTableLike(tableRow.table);
        while (null != tableRow) {
            table.append(tableRow.table, tableRow.row);
            if (table.size() >= maxTableSize) {
                break;
            }
            tableRow = blockingQueue.poll();
        }
        return table;
    }

    public Table consumeBatch(int myThreadIndex) throws InterruptedException {
        BlockingQueue<TableRow> blockingQueue = blockingQueueInThread.get(myThreadIndex);
        ifHashUneven(blockingQueue);
        TableRow tableRow;
        while ((tableRow = blockingQueue.poll(100, TimeUnit.MILLISECONDS)) == null);
        Table table = Table.createEmptyTableLike(tableRow.table);
        table.append(tableRow.table, tableRow.row);
        while (table.size() < maxTableSize) {
            while ((tableRow = blockingQueue.poll(100, TimeUnit.MILLISECONDS)) == null);
            table.append(tableRow.table, tableRow.row);
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
