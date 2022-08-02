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

    private final Duration requestTimeout = Duration.ofSeconds(10);
    private final int thread;
    private final int toServer;
    private final String uniqueName;
    private final Client[] clients;
    private final int batchSize;
    private final List<BlockingQueue<Rehash.TableRow>> blockingQueue;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final Node node;
    private final String hostPort;

    RehashOutputTable(String uniqueName, int toServer, int rehashThreadNum, int queueSize) {
        this(uniqueName, 5_0000, toServer, rehashThreadNum, queueSize);
    }

    RehashOutputTable(String uniqueName, int batchSize, int toServer, int rehashThreadNum, int queueSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException();
        }
        this.batchSize = batchSize;
        clients = new Client[rehashThreadNum];
        node = SystemProperty.getNodeByHash(toServer);
        for (int j = 0; j < rehashThreadNum; j++) {
            clients[j] = newClient(node.getHost(), node.getPort());
        }
        hostPort = node.getHost() + ":" + node.getPort();

        this.thread = rehashThreadNum;
        this.toServer = toServer;
        this.uniqueName = requireNonNull(uniqueName);
        this.blockingQueue = new ArrayList<>(rehashThreadNum);
        for (int i = 0; i < rehashThreadNum; i++) {
            this.blockingQueue.add(new ArrayBlockingQueue<>(queueSize));
        }
        addQueueSizeLog(uniqueName + "-out-to-server-" + node.getHost() + ":" + node.getPort(), blockingQueue);
        threadPoolExecutor = new ThreadPoolExecutor(rehashThreadNum,
                rehashThreadNum,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder()
                        .setNameFormat(uniqueName + "-rehash-output-%d")
                        .build());
    }

    public void request(String command, Object... args) throws InterruptedException {
        clients[0].asyncRequest(command, args);
    }

    public void produce(Table table, int row, int toThread) throws InterruptedException {
        while (!blockingQueue.get(toThread).offer(new Rehash.TableRow(table, row), 5, TimeUnit.SECONDS)) {
            logger.warn(format("exceed 5 seconds cannot offer to %s out queue", uniqueName));
        }
    }

    public void start() {
        for (int i = 0; i < thread; i++) {
            final int finalI = i;
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Table tmp = null;
                    long lastFlushTime = System.currentTimeMillis();
                    while (!Thread.interrupted()) {
                        try {
                            Rehash.TableRow tableRow = blockingQueue.get(finalI).poll(100, TimeUnit.MILLISECONDS);
                            if (null == tableRow) {
                                continue;
                            }
                            if (null == tmp) {
                                tmp = Table.createEmptyTableLike(tableRow.table);
                            }
                            tmp.append(tableRow.table, tableRow.row);
                            if (tmp.size() >= batchSize) {
                                request(finalI, tmp, finalI);
                                tmp = Table.createEmptyTableLike(tableRow.table);
                            }
                            long now = System.currentTimeMillis();
                            if (now - lastFlushTime > 1000) {
                                request(finalI, tmp, finalI);
                                tmp = Table.createEmptyTableLike(tableRow.table);
                                lastFlushTime = now;
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
        int j = 0;
        int retryTimes = 3;
        while (true) {
            try {
                return clients[thread].asyncRequest("rehash",
                        uniqueName,
                        toThread,
                        table);
            } catch (Throwable t) {
                if (!"request timeout".equals(t.getMessage())) {
                    i++;
                    logger.error(format("request to %s error %d times", hostPort, i), t);
                    if (i >= retryTimes) {
                        return -2;
                    }
                } else {
                    j++;
                    logger.warn("request to {} time out {} times", hostPort, j);
                }
                try {
                    Thread.sleep(5000);
                    clients[thread].close();
                    clients[thread] = newClient(node.getHost(), node.getPort());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void request(int thread, Table table, int toThread) throws InterruptedException {
        if (null != table && table.size() > 0) {
            int ret = requestWithRetry(thread, table, toThread);
            if (ret != table.size()) {
                String msg = format("%s received size: %d not equal to table.size: %d", hostPort, ret, table.size());
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
