package io.github.shanqiang.sp.input;

import io.github.shanqiang.criteria.JoinCriteria;
import io.github.shanqiang.function.AggTimeWindowFunction;
import io.github.shanqiang.function.TimeWindowFunction;
import io.github.shanqiang.sp.Compute;
import io.github.shanqiang.sp.Rehash;
import io.github.shanqiang.sp.StreamProcessing;
import io.github.shanqiang.sp.dimension.MysqlDimensionTable;
import io.github.shanqiang.sp.dimension.TableIndex;
import io.github.shanqiang.sp.output.KafkaOutputTable;
import io.github.shanqiang.table.As;
import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Row;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import io.github.shanqiang.util.AggregationUtil;
import io.github.shanqiang.util.WindowUtil;
import io.github.shanqiang.window.SessionWindow;
import io.github.shanqiang.window.SlideWindow;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Top100Test {
    /**
     * cd kafka_2.13-2.8.0
     * bin/zookeeper-server-start.sh config/zookeeper.properties
     * bin/kafka-server-start.sh config/server.properties
     * bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic my_topic_name --partitions 1
     */

    private void produce(String bootstrapServers, String topic, Map<String, Type> columnTypeMap) throws InterruptedException {
        KafkaOutputTable kafkaOutputTable = new KafkaOutputTable(1, bootstrapServers, topic);
        kafkaOutputTable.start();
        Table table = new TableBuilder(columnTypeMap)
                .append(0, 0L)
                .append(1, 1L)
                .append(2, 1)
                .append(3, 3)

                .append(0, 3000L)
                .append(1, 2L)
                .append(2, 3)
                .append(3, 2)

                .append(0, 50000L)
                .append(1, 3L)
                .append(2, 2)
                .append(3, 1)

                .append(0, 3599000L)
                .append(1, 4L)
                .append(2, 3)
                .append(3, 1)

                .append(0, 3600000L)
                .append(1, 5L)
                .append(2, 2)
                .append(3, 1)

                .append(0, 5399000L)
                .append(1, 6L)
                .append(2, 1)
                .append(3, 5)

                .build();
        kafkaOutputTable.produce(table);
    }

    @Test
    public void top100() throws InterruptedException {
        /**
         * create database ecommerce;
         * CREATE USER 'userName'@'localhost' IDENTIFIED BY 'password';
         * GRANT ALL ON *.* TO 'userName'@'localhost';
         * use ecommerce;
         * create table commodity(id int not null  auto_increment, name varchar(40), price int, primary key (id));
         *
         */
        MysqlDimensionTable mysqlDimensionTable = new MysqlDimensionTable("jdbc:mysql://localhost:3306/ecommerce",
                "commodity",
                "userName",
                "password",
                Duration.ofHours(1),
                new ColumnTypeBuilder()
                        .column("id", Type.INT)
                        .column("name", Type.VARBYTE)
                        .column("price", Type.INT)
                        .build(),
                "id"
        );

        Map<String, Type> columnTypeMap = new ColumnTypeBuilder()
                .column("__time__", Type.BIGINT)
                .column("id", Type.BIGINT)
                .column("commodity_id", Type.INT)
                .column("count", Type.INT)
                .build();
        String bootstrapServers = "localhost:9092";
        String topic = "order";
        produce(bootstrapServers, topic, columnTypeMap);

        KafkaStreamTable kafkaStreamTable = new KafkaStreamTable(bootstrapServers,
                "consumerGroupId",
                topic,
                0,
                columnTypeMap);
        kafkaStreamTable.start();

        StreamProcessing sp = new StreamProcessing(1);
        String[] hashBy = new String[]{"commodity_id"};
        Rehash rehashForSlideWindow = sp.rehash("uniqueNameForSlideWindow", hashBy);
        String[] returnedColumns = new String[]{"commodity_id",
                "sales_volume",
                "window_start"};
        SlideWindow slideWindow = new SlideWindow(Duration.ofHours(1),
                Duration.ofMinutes(30),
                hashBy,
                "__time__",
                new AggTimeWindowFunction() {
                    @Override
                    public Comparable[] agg(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd) {
                        return new Comparable[]{
                                partitionByColumns.get(0),
                                AggregationUtil.sumInt(rows, "count"),
                                windowStart
                        };
                    }
                }, returnedColumns);
        slideWindow.setWatermark(Duration.ofSeconds(2));

        hashBy = new String[]{"window_start"};
        Rehash rehashForSessionWindow = sp.rehash("uniqueNameForSessionWindow", hashBy);
        SessionWindow sessionWindow = new SessionWindow(Duration.ofSeconds(1),
                hashBy,
                "window_start",
                new TimeWindowFunction() {
                    @Override
                    public List<Comparable[]> transform(List<Comparable> partitionByColumns, List<Row> rows, long windowStart, long windowEnd) {
                        int[] top100 = WindowUtil.topN(rows, "sales_volume", 100);
                        List<Comparable[]> ret = new ArrayList<>(100);
                        for (int i = 0; i < top100.length; i++) {
                            ret.add(rows.get(top100[i]).getAll());
                        }
                        return ret;
                    }
                }, returnedColumns);
        sessionWindow.setWatermark(Duration.ofSeconds(3));

        sp.compute(new Compute() {
            @Override
            public void compute(int myThreadIndex) throws InterruptedException {
                Table table = kafkaStreamTable.consume();
                TableIndex tableIndex = mysqlDimensionTable.curTable();
                table = table.leftJoin(tableIndex.getTable(), new JoinCriteria() {
                            @Override
                            public List<Integer> theOtherRows(Row thisRow) {
                                // Use tableIndex.getRows but not mysqlDimensionTable.curTable().getRows. Consider the second
                                // mysqlDimensionTable.curTable() may correspond to the newly reloaded dimension table which
                                // is not consistent with the first mysqlDimensionTable.curTable() and tableIndex.getTable()
                                return tableIndex.getRows(thisRow.getInteger("commodity_id"));
                            }
                        },
                        new As().
                                as("id", "order_id").
                                build(),
                        new As().
                                as("name", "commodity_name").
                                as("price", "commodity_price").
                                build());
                List<Table> tables = rehashForSlideWindow.rehash(table, myThreadIndex);
                table = slideWindow.slide(tables);
                tables = rehashForSessionWindow.rehash(table, myThreadIndex);
                table = sessionWindow.session(tables);
                if (table.size() > 0) {
                    table.print();
                    assert table.size() == 3;
                    assert table.getColumn("commodity_id").getInteger(0) == 3;
                    assert table.getColumn("commodity_id").getInteger(2) == 2;
                    assert table.getColumn("window_start").getLong(0) == 0L;
                    //elegantly finish the streaming task when terminate condition is satisfied
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
}