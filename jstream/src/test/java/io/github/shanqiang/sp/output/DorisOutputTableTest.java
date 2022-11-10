//package io.github.shanqiang.sp.output;
//
//import io.github.shanqiang.sp.input.MysqlFetcher;
//import io.github.shanqiang.table.ColumnTypeBuilder;
//import io.github.shanqiang.table.Table;
//import io.github.shanqiang.table.TableBuilder;
//import io.github.shanqiang.table.Type;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.sql.SQLException;
//import java.time.Duration;
//import java.util.Map;
//
//public class DorisOutputTableTest {
//    @Test
//    public void test() throws IOException, InterruptedException, SQLException {
//        String hostPort = "10.209.21.229:8031";
//        Map<String, Type> columnTypeMap = new ColumnTypeBuilder()
//                .column("c1", Type.INT)
//                .column("c2", Type.VARBYTE)
//                .column("c3", Type.BIGINT)
//                .build();
//        DorisOutputTable dorisOutputTable = new DorisOutputTable(
//                1,
//                40000,
//                Duration.ofSeconds(1),
//                hostPort,
//                "10.209.21.229:8030",
//                "root",
//                "",
//                "ecommerce",
//                "test",
//                new String[]{"c1"},
//                1,
//                true,
//                2,
//                "c3",
//                6,
//                1,
//                "SSD",
//                columnTypeMap
//        );
//        dorisOutputTable.start();
//
//        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
//        tableBuilder.append(0, 1);
//        tableBuilder.append(1, "c2v1");
//        tableBuilder.append(2, System.currentTimeMillis());
//        tableBuilder.append(0, 2);
//        tableBuilder.append(1, "c2v2");
//        tableBuilder.append(2, System.currentTimeMillis());
//        dorisOutputTable.produce(tableBuilder.build());
//
//        dorisOutputTable.stop();
//
//        Thread.sleep(1000);
//
//        MysqlFetcher mysqlFetcher = new MysqlFetcher(
//                "jdbc:mysql://" + hostPort + "/ecommerce",
//                "root",
//                "",
//                columnTypeMap);
//        Table table = mysqlFetcher.fetch("select * from test");
//        assert table.size() == 2;
//    }
//}
