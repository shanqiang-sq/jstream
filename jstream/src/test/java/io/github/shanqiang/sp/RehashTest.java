package io.github.shanqiang.sp;

import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import io.github.shanqiang.window.TimeWindowTest;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

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

        List<Table> tableList1 = rehash.rehash(tableBuilder.build(), 1);
        assert tableList1.get(0).size() == 0;

        List<Table> tableList2 = rehash.rehash(tableBuilder.build(), 0);
        assert tableList2.get(0).size() == N;
        assert tableList2.get(1).size() == N;

        long end = System.currentTimeMillis();
        System.out.println(String.format("%f", (end - start) / 1000.0));
        assert end - start < 2000;

        rehash.close();
    }
}