package io.github.shanqiang.sp;

import io.github.shanqiang.Threads;
import io.github.shanqiang.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class QueueSizeLogger {
    private static final Logger logger = LoggerFactory.getLogger(QueueSizeLogger.class);

    private enum Type {
        QUEUE_SIZE,
        RECORD_SIZE
    }

    private static class CollectionType {
        private final Collection collection;
        private final Type type;

        private CollectionType(Collection collection, Type type) {
            this.collection = collection;
            this.type = type;
        }
    }

    private static final Map<String, CollectionType> stats = new ConcurrentHashMap();

    static {
        new ScheduledThreadPoolExecutor(1, Threads.threadsNamed("QueueSizeLogger")).scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (String name : stats.keySet()) {
                    CollectionType collectionType = stats.get(name);
                    switch (collectionType.type) {
                        case QUEUE_SIZE:
                            logger.info("{}: {sum: {}, max: {}, min: {}}"
                                    , name
                                    , sumQueueSize(collectionType.collection)
                                    , maxQueueSize(collectionType.collection)
                                    , minQueueSize(collectionType.collection)
                            );
                            break;
                        case RECORD_SIZE:
                            logger.info("{}: {}", name, computeRecordSize(collectionType.collection));
                            break;
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private QueueSizeLogger() {
    }

    public static void addQueueSizeLog(String name, Collection collections) {
        stats.put(name, new CollectionType(collections, Type.QUEUE_SIZE));
    }

    public static void addRecordSizeLog(String name, Collection collections) {
        stats.put(name, new CollectionType(collections, Type.RECORD_SIZE));
    }

    private static long sumQueueSize(Collection collections) {
        long sum = 0;
        for (Object collection : collections) {
            sum += ((Collection) collection).size();
        }
        return sum;
    }

    private static long maxQueueSize(Collection collections) {
        long max = 0;
        for (Object collection : collections) {
            long tmp = ((Collection) collection).size();
            if (tmp > max) {
                max = tmp;
            }
        }
        return max;
    }

    private static long minQueueSize(Collection collections) {
        long min = Long.MAX_VALUE;
        for (Object collection : collections) {
            long tmp = ((Collection) collection).size();
            if (tmp < min) {
                min = tmp;
            }
        }
        return min;
    }

    private static long computeRecordSize(Collection collections) {
        long sum = 0;
        for (Object collection : collections) {
            ArrayBlockingQueue<Table> queue = (ArrayBlockingQueue<Table>) collection;
            for (Table table : queue) {
                sum += table.size();
            }
        }
        return sum;
    }
}
