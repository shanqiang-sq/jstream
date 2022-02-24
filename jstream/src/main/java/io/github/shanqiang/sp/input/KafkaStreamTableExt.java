package io.github.shanqiang.sp.input;

import com.google.gson.Gson;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import static java.util.Arrays.asList;

public class KafkaStreamTableExt extends KafkaStreamTable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamTableExt.class);

    /**
     * 消费到的表结构：第一列timestamp 第二列key 第三列value
     * @param bootstrapServers
     * @param consumerGroupId
     * @param topic
     * @param keyDeserializer
     * @param valueDeserializer
     * @param consumeFrom
     * @param consumeTo         -1 denote infinite
     * @param columnTypeMap
     */
    public KafkaStreamTableExt(String bootstrapServers,
                               String consumerGroupId,
                               String topic,
                               String keyDeserializer,
                               String valueDeserializer,
                               long consumeFrom,
                               long consumeTo,
                               Map<String, Type> columnTypeMap) {
        super(bootstrapServers, consumerGroupId, topic, keyDeserializer, valueDeserializer, consumeFrom, consumeTo, columnTypeMap);
    }

    protected void newConsumer(TopicPartition topicPartition, OffsetAndTimestamp offsetAndTimestamp) {
        if (topicPartition.partition() % serverCount != myHash) {
            return;
        }
        if (myPartitions.contains(topicPartition.partition())) {
            return;
        }
        myPartitions.add(topicPartition.partition());
        addPartition(topicPartition.partition());
        int threadId = arrayBlockingQueueList.size();
        arrayBlockingQueueList.add(new ArrayBlockingQueue<>(queueDepth));
        KafkaStreamTableExt kafkaStreamTable = this;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Consumer<Integer, String> consumer = new KafkaConsumer<>(properties);
                consumer.assign(asList(topicPartition));
                if (null == offsetAndTimestamp) {
                    consumer.seekToBeginning(asList(topicPartition));
                } else {
                    consumer.seek(topicPartition, offsetAndTimestamp.offset());
                }

                Gson gson = new Gson();
                while (!Thread.interrupted()) {
                    try {
                        ConsumerRecords records = consumer.poll(Duration.ofMillis(sleepMs));
                        if (records.isEmpty()) {
                            continue;
                        }
                        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
                        for (Object obj : records) {
                            ConsumerRecord record = (ConsumerRecord) obj;
                            tableBuilder.append(0, record.timestamp());
                            tableBuilder.appendValue(1, record.key());
                            tableBuilder.appendValue(2, record.value());
                        }
                        queueSizeLogger.logQueueSize("input queue size" + kafkaStreamTable.sign, arrayBlockingQueueList);
                        recordSizeLogger.logRecordSize("input queue rows" + kafkaStreamTable.sign, arrayBlockingQueueList);
                        arrayBlockingQueueList.get(threadId).put(tableBuilder.build());
                    } catch (InterruptException e) {
                        break;
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        });
        thread.start();
        consumers.add(thread);
    }
}