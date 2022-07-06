package io.github.shanqiang.sp.output;

import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;

import static io.github.shanqiang.sp.QueueSizeLogger.addQueueSizeLog;
import static io.github.shanqiang.sp.QueueSizeLogger.addRecordSizeLog;
import static java.lang.Integer.toHexString;
import static java.util.Objects.requireNonNull;

public abstract class AbstractOutputTable implements OutputTable {
    protected static final String __time__ = "__time__";

    protected final List<ArrayBlockingQueue<Table>> arrayBlockingQueueList;
    protected final int thread;
    protected final int queueDepth = 100;
    protected final Table emptyTable;
    protected final String sign;
    private final Random random = new Random();

    protected AbstractOutputTable(int thread, String sign) {
        this.thread = thread;
        arrayBlockingQueueList = new ArrayList<>(thread);
        for (int i = 0; i < thread; i++) {
            arrayBlockingQueueList.add(new ArrayBlockingQueue<>(queueDepth));
        }
        this.emptyTable = new TableBuilder(new ColumnTypeBuilder()
                .column("no_use", Type.INT)
                .build())
                .build();
        this.sign = requireNonNull(sign);

        addQueueSizeLog("输出队列大小" + sign + "|" + toHexString(hashCode()), arrayBlockingQueueList);
        addRecordSizeLog("输出队列行数" + sign + "|" + toHexString(hashCode()), arrayBlockingQueueList);
    }

    protected void putTable(Table table) throws InterruptedException {
        arrayBlockingQueueList.get(random()).put(table);
    }

    final protected Table consume() throws InterruptedException {
        while (true) {
            for (ArrayBlockingQueue<Table> arrayBlockingQueue : arrayBlockingQueueList) {
                Table table = arrayBlockingQueue.poll();
                if (null != table) {
                    return table;
                }
            }

            Thread.sleep(100);
            return emptyTable;
        }
    }

    private int random() {
        return random.nextInt(thread);
    }
}
