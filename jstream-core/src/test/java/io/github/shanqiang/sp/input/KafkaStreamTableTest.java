package io.github.shanqiang.sp.input;

import io.github.shanqiang.sp.output.KafkaOutputTable;
import io.github.shanqiang.table.ColumnTypeBuilder;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import org.junit.Test;

import java.util.Calendar;
import java.util.Map;

public class KafkaStreamTableTest {
    /**
     * cd kafka_2.13-2.8.0
     * bin/zookeeper-server-start.sh config/zookeeper.properties
     * bin/kafka-server-start.sh config/server.properties
     * bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic my_topic_name --partitions 1
     */

    @Test
    public void produceThenConsume() throws InterruptedException {
        long now = System.currentTimeMillis();

        Map<String, Type> columnTypeMap = new ColumnTypeBuilder()
                .column("testColumnVarchar", Type.VARBYTE)
                .column("testColumnInt", Type.INT)
                .column("testColumnBigint", Type.BIGINT)
                .column("testColumnDouble", Type.DOUBLE)
                .build();
        String bootstrapServers = "localhost:9092";
        String topic = "testTopic";
        KafkaOutputTable kafkaOutputTable = new KafkaOutputTable(bootstrapServers, topic);
        kafkaOutputTable.start();
        Table table = new TableBuilder(columnTypeMap)
                .append(0, "c1v1")
                .append(1, 1)
                .append(2, Long.MAX_VALUE)
                .append(3, Double.MAX_VALUE)
                .appendValue(0, null)
                .append(1, 2)
                .append(2, Long.MIN_VALUE)
                .append(3, Double.MIN_VALUE)
                .build();
        kafkaOutputTable.produce(table);

        columnTypeMap = new ColumnTypeBuilder()
                .column("testColumnVarchar", Type.VARBYTE)
                .column("testColumnInt", Type.INT)
                .column("testColumnBigint", Type.BIGINT)
                .column("__time__", Type.BIGINT)
                .column("__receive_time__", Type.BIGINT)
                .column("testColumnDouble", Type.DOUBLE)
                .build();
        KafkaStreamTable kafkaStreamTable = new KafkaStreamTable(bootstrapServers,
                "consumerGroupId",
                topic,
                now,
                new Calendar.Builder().
                        setDate(9999, 9, 17).
                        setTimeOfDay(11, 3, 0).
                        build().
                        getTimeInMillis(),
                columnTypeMap);
        kafkaStreamTable.start();
        Thread.sleep(5_000);
        table = kafkaStreamTable.consume();

        assert table.size() == 2;
        assert table.getColumn("__receive_time__").getLong(0)
                >=
                table.getColumn("__time__").getLong(0);
        assert table.getColumn(0).getString(0).equals("c1v1");
        assert table.getColumn(1).getInteger(0).equals(1);
        assert table.getColumn(2).getLong(0).equals(Long.MAX_VALUE);
        assert table.getColumn(0).getString(1) == null;
        assert table.getColumn(5).getDouble(1) == Double.MIN_VALUE;

        columnTypeMap = new ColumnTypeBuilder()
                .column("__receive_time__", Type.BIGINT)
                .column("__time__", Type.BIGINT)
                .column("value", Type.VARBYTE)
                .build();
        KafkaStreamTableExt kafkaStreamTableExt = new KafkaStreamTableExt(bootstrapServers,
                "consumerGroupId",
                topic,
                "org.apache.kafka.common.serialization.LongDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                now,
                new Calendar.Builder().
                        setDate(9999, 9, 17).
                        setTimeOfDay(11, 3, 0).
                        build().
                        getTimeInMillis(),
                columnTypeMap);
        kafkaStreamTableExt.start();
        Thread.sleep(1_000);
        table = kafkaStreamTableExt.consume();
        assert table.size() == 2;

        assert table.getColumn("__receive_time__").getLong(0)
                >=
                table.getColumn("__time__").getLong(0);

        kafkaStreamTableExt = new KafkaStreamTableExt(bootstrapServers,
                "consumerGroupId",
                topic,
                "org.apache.kafka.common.serialization.LongDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                System.currentTimeMillis(),
                new Calendar.Builder().
                        setDate(9999, 9, 17).
                        setTimeOfDay(11, 3, 0).
                        build().
                        getTimeInMillis(),
                columnTypeMap);
        kafkaStreamTableExt.start();
        Thread.sleep(1_000);
        table = kafkaStreamTableExt.consume();
        assert table.size() == 0;
    }
}