package io.github.shanqiang.sp.output;

import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class DorisOutputTableTest {
    @Test
    public void test() throws IOException, InterruptedException {
//        Map<String, Type> columnTypeMap = new ColumnTypeBuilder()
//                .column("c1", Type.INT)
//                .column("c2", Type.VARBYTE)
//                .build();
//        DorisOutputTable dorisOutputTable = new DorisOutputTable(
//                "localhost:3306",
//                "localhost:3306",
//                "root",
//                "",
//                "ecommerce",
//                "test",
//                new String[]{"c1"},
//                columnTypeMap
//        );
//        dorisOutputTable.start();
//
//        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
//        tableBuilder.append(0, 1);
//        tableBuilder.append(1, "c2v1");
//        tableBuilder.append(0, 2);
//        tableBuilder.append(1, "c2v2");
//        dorisOutputTable.produce(tableBuilder.build());
//
//        Thread.sleep(3_000);
    }
}
