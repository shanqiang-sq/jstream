package io.github.shanqiang.window;

import io.github.shanqiang.function.ReduceTimeWindowFunction;
import io.github.shanqiang.sp.Compute;
import io.github.shanqiang.sp.Rehash;
import io.github.shanqiang.sp.StreamProcessing;
import io.github.shanqiang.sp.input.InsertableStreamTable;
import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Row;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.Type;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SlideWindowReduceTest {
    private static final Logger logger = LoggerFactory.getLogger(SlideWindowReduceTest.class);

    @Test
    public void slide() {
        Map<String, Type> columnTypeMap = new ColumnTypeBuilder().
                column("firstPartitionByColumn", Type.VARBYTE).
                column("secondPartitionByColumn", Type.VARBYTE).
                column("ts", Type.BIGINT).
                build();
        InsertableStreamTable insertableStreamTable = new InsertableStreamTable(1, columnTypeMap);
        insertableStreamTable.sleepMsWhenNoData(2000);
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 3L);

        //下面两个表time字段值都是10，watermark之后会成为2行记录的一个表
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 10L);
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 10L);

        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 13L);

        //下面这条记录触发了上面3条记录在[5,15)的计算然后窗口前进到[10,20),
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 23L);
        insertableStreamTable.insert(0, "firstPartitionByColumn1", "secondPartitionByColumn1", 24L);

        //下面这条记录触发了上面time=23的那条记录的计算（windowTimeout=10ms）
        //这条记录之后没有数据了所以要等到noDataDelay（2000ms）之后才会触发这条记录的计算
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 103L);

        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 2L);
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 301L);
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 302L);
        insertableStreamTable.insert(0, "firstPartitionByColumn2", "secondPartitionByColumn2", 303L);

        StreamProcessing sp = new StreamProcessing(1, insertableStreamTable);
        SlideWindow slideWindow = new SlideWindow(Duration.ofMillis(10),
                Duration.ofMillis(5),
                new String[]{"firstPartitionByColumn", "secondPartitionByColumn"},
                "ts",
                new ReduceTimeWindowFunction() {
                    @Override
                    public Comparable[] agg(Comparable[] preAggResult, List<Comparable> partitionByColumns, Row newRow, long windowStart, long windowEnd) {
                        long sum = newRow.getLong("ts");
                        if (null != preAggResult) {
                            sum += (long) preAggResult[2];
                        }
                        return new Comparable[]{
                                partitionByColumns.get(0),
                                partitionByColumns.get(1),
                                sum,
                                windowStart,
                                windowEnd
                        };
                    }

                    @Override
                    public Comparable[] aggEnd(Comparable[] preAggResult, List<Comparable> partitionByColumns, long windowStart, long windowEnd) {
                        logger.info("{}", Arrays.toString(preAggResult));
                        return ReduceTimeWindowFunction.super.aggEnd(preAggResult, partitionByColumns, windowStart, windowEnd);
                    }
                }, "firstPartitionByColumn", "secondPartitionByColumn", "ts", "windowStart", "windowEnd");
        slideWindow.setWatermark(Duration.ofMillis(0));

        StreamProcessing sp2 = new StreamProcessing(1);
        Rehash rehash = new Rehash(sp2, "rehash1", "firstPartitionByColumn", "secondPartitionByColumn");
        sp.computeNoWait(new Compute() {
            @Override
            public void compute(int myThreadIndex) throws InterruptedException {
                rehash.rehash(insertableStreamTable.consume());
            }
        });

        List<Table> tables = new ArrayList<>();
        AtomicInteger atomicInteger = new AtomicInteger();
        sp2.compute(new Compute() {
            @Override
            public void compute(int myThreadIndex) throws InterruptedException {
                Table table = rehash.consume(myThreadIndex);
                table = slideWindow.slide(table);
                if (table.size() > 0) {
                    for (int i = 0; i < table.size(); i++) {
                        logger.info("{}, {}, {}, {}, {}, {}",
                                table,
                                table.getColumn(0).get(i),
                                table.getColumn(1).get(i),
                                table.getColumn(2).get(i),
                                table.getColumn(3).get(i),
                                table.getColumn(4).get(i));
                    }
                    tables.add(table);
                }
                if (9 == atomicInteger.addAndGet(table.size())) {
                    throw new InterruptedException();
                }
            }
        });

        assert tables.get(0).getColumn("ts").getLong(2) == 33;  // 10 + 10 + 13
        assert tables.get(0).getColumn("ts").getLong(8) == 303;
    }
}