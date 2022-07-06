package io.github.shanqiang.sp;

import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RehashTest {

    /**
     * manual test for "triggered full gc" case
     */
    public void test() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            rehash();
        }
    }

    @Test
    public void rehash() throws InterruptedException {
        StreamProcessing streamProcessing = new StreamProcessing(2);
        Rehash rehash = streamProcessing.rehash("rehash1", "c1");

        Map<String, Type> columnTypeMap = new ColumnTypeBuilder().
                column("c1", Type.VARBYTE).
                column("ts", Type.BIGINT).
                build();
        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);

        long start = System.currentTimeMillis();
        final int N = 1000_000;
        for (int i = 0; i < N; i++) {
            tableBuilder.append(0, "c1v1");
            tableBuilder.append(1, System.currentTimeMillis());
        }
        final Table finalTable = tableBuilder.build();

        BlockingQueue<Table> blockingQueue = new ArrayBlockingQueue<>(2);
        blockingQueue.add(finalTable);
        blockingQueue.add(finalTable);

        int[] a = new int[2];
        streamProcessing.compute(new Compute() {
            @Override
            public void compute(int myThreadIndex) throws InterruptedException {
                Table table = blockingQueue.poll();
                if (null == table) {
                    table = Table.createEmptyTableLike(finalTable);
                }
                List<Table> tableList1 = rehash.rehash(table, myThreadIndex);
                a[myThreadIndex] += tableList1.get(0).size();
                if (a[0] >= 2 * N) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        assert a[0] == 2 * N;
        assert a[1] == 0;

        long end = System.currentTimeMillis();
        System.out.println(String.format("%f", (end - start) / 1000.0));
        assert end - start < 2000;

        rehash.close();
    }
}