package io.github.shanqiang.sp;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.shanqiang.SystemProperty;
import io.github.shanqiang.network.client.Client;
import io.github.shanqiang.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.github.shanqiang.sp.QueueSizeLogger.addQueueSizeLog;
import static io.github.shanqiang.sp.StreamProcessing.handleException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RehashOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(RehashOutputTable.class);

    // 永不超时,通过 addQueueSizeLog 监控
    private final Duration requestTimeout = Duration.ofDays(50 * 365);
    private final int thread;
    private final int toServer;
    private final String uniqueName;
    private final Client[] clients;
    private final int batchSize = 80000;
    private final List<BlockingQueue<Rehash.TableRow>> blockingQueue;
    private final ThreadPoolExecutor threadPoolExecutor;

    RehashOutputTable(String uniqueName, int thread, int toServer, int rehashThreadNum, int queueSize) {
        this.thread = thread;
        this.toServer = toServer;
        this.uniqueName = requireNonNull(uniqueName);
        this.blockingQueue = new ArrayList<>(rehashThreadNum);
        for (int i = 0; i < rehashThreadNum; i++) {
            this.blockingQueue.add(new ArrayBlockingQueue<>(queueSize));
        }
        addQueueSizeLog(uniqueName + "-out-to-server-" + toServer, blockingQueue);
        threadPoolExecutor = new ThreadPoolExecutor(thread,
                thread,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder().setNameFormat(uniqueName + "-rehash-output-%d").build());
        if (null != SystemProperty.getSelf()) {
            clients = new Client[thread];
            Node node = SystemProperty.getNodeByHash(toServer);
            for (int j = 0; j < thread; j++) {
                clients[j] = newClient(node.getHost(), node.getPort());
            }
        } else {
            clients = new Client[0];
        }
    }

    public void request(String command, Object... args) throws InterruptedException {
        clients[0].request(command, args);
    }

    public void produce(Table table, int row, int toThread) throws InterruptedException {
//        return blockingQueue.get(toThread).offer(new Rehash.TableRow(table, row), 100, TimeUnit.MILLISECONDS);
        blockingQueue.get(toThread).put(new Rehash.TableRow(table, row));
    }

    public void start() {
        for (int i = 0; i < thread; i++) {
            final int finalI = i;
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    while (!Thread.interrupted()) {
                        try {
                            boolean allIsNull = true;
                            for (int j = 0; j < blockingQueue.size(); j++) {
                                Rehash.TableRow tableRow = blockingQueue.get(j).poll();
                                if (null != tableRow) {
                                    allIsNull = false;
                                    Table tmp = Table.createEmptyTableLike(tableRow.table);
                                    while (null != tableRow) {
                                        tmp.append(tableRow.table, tableRow.row);
                                        if (tmp.size() % batchSize == 0) {
                                            request(finalI, tmp, j);
                                            tmp = Table.createEmptyTableLike(tableRow.table);
                                        }
                                        tableRow = blockingQueue.get(j).poll();
                                    }
                                    request(finalI, tmp, j);
                                }
                            }
                            if (allIsNull) {
                                Thread.sleep(100);
                            }
                        } catch (InterruptedException e) {
                            logger.info("interrupted");
                            break;
                        } catch (Throwable t) {
                            handleException(t);
                            break;
                        }
                    }
                }
            });
        }
    }

    public void stop() {
        threadPoolExecutor.shutdownNow();
        for (int j = 0; j < thread; j++) {
            clients[j].close();
        }
    }

    private int requestWithRetry(int thread, Table table, int toThread) {
        int i = 0;
        int retryTimes = 1;
        while (true) {
            try {
                return clients[thread].request("rehash",
                        uniqueName,
                        toThread,
                        table);
            } catch (Throwable t) {
                i++;
                logger.error(format("request error %d times", i), t);
                if (i >= retryTimes) {
                    return -2;
                }
                try {
                    Thread.sleep(5000);
                    Node node = SystemProperty.getNodeByHash(toServer);
                    clients[thread].close();
                    clients[thread] = newClient(node.getHost(), node.getPort());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void request(int thread, Table table, int toThread) {
        if (null != table && table.size() > 0) {
            int ret = requestWithRetry(thread, table, toThread);
            if (ret != table.size()) {
                String msg = format("the peer received size: %d not equal to table.size: %d", ret, table.size());
                throw new IllegalStateException(msg);
            }
        }
    }

    private Client newClient(String host, int port) {
        while (true) {
            try {
                return new Client(false, host, port, requestTimeout);
            } catch (Throwable t) {
                try {
                    Thread.sleep(1000);
                    logger.info("retry to connect to " + host + ":" + port);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("interrupted");
                }
            }
        }
//        throw new RuntimeException(format("cannot create client to host: %s, port: %d", host, port));
    }
}
