package io.github.shanqiang.sp.input;

import io.github.shanqiang.exception.UnknownTypeException;
import io.github.shanqiang.offheap.ByteArray;
import io.github.shanqiang.sp.Delay;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.github.shanqiang.SystemProperty;
import io.github.shanqiang.Threads;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.github.shanqiang.sp.input.kafka.MyKafkaConsumer.newKafkaConsumer;
import static java.lang.Integer.toHexString;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class KafkaStreamTable extends AbstractStreamTable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamTable.class);

    protected final Properties properties;
    private final String topic;
    protected final String sign;
    private final long consumeFrom;
    protected final long consumeTo;
    protected final int myHash;
    protected final int serverCount;
    protected final Set<Integer> myPartitions = new HashSet<>();
    private final ScheduledExecutorService partitionsDetector;
    protected final List<Thread> consumers = new ArrayList<>();
    private final int timeColumnIndex;
    private final int receiveTimeColumnIndex;
    private final List<String> stringColumns;
    private final List<Type> types;
    private long finishDelayMs = 30000;
    private long lastUpdateMs = System.currentTimeMillis();
    private final Set<Integer> partitionSet = new HashSet<>();
    //由于partitionSet.size()读取非常频繁且计算代价比较大使用partitionSetSize缓存该值
    private int partitionSetSize;

    public KafkaStreamTable(String bootstrapServers,
                            String consumerGroupId,
                            String topic,
                            long consumeFrom,
                            Map<String, Type> columnTypeMap) {
        this(bootstrapServers, consumerGroupId, topic, consumeFrom, -1, columnTypeMap);
    }

    /**
     * key是kafka客户端写kafka的时间（int型，秒），kafka服务端的timestamp是接收到数据的时间据此也可评估写入latency
     * value是一个json格式的字符串
     * @param bootstrapServers
     * @param consumerGroupId
     * @param topic
     * @param consumeFrom
     * @param consumeTo
     * @param columnTypeMap
     */
    public KafkaStreamTable(String bootstrapServers,
                            String consumerGroupId,
                            String topic,
                            long consumeFrom,
                            long consumeTo,
                            Map<String, Type> columnTypeMap) {
        this(bootstrapServers, consumerGroupId, topic,
                "org.apache.kafka.common.serialization.LongDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                consumeFrom, consumeTo, columnTypeMap);
    }

    protected KafkaStreamTable(String bootstrapServers,
                               String consumerGroupId,
                               String topic,
                               String keyDeserializer,
                               String valueDeserializer,
                               long consumeFrom,
                               long consumeTo,
                               Map<String, Type> columnTypeMap) {
        super(0, columnTypeMap);
        this.topic = requireNonNull(topic);
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, requireNonNull(bootstrapServers));
        properties.put(GROUP_ID_CONFIG, requireNonNull(consumerGroupId));
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        properties.put(MAX_POLL_RECORDS_CONFIG, 40000);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(AUTO_OFFSET_RESET_CONFIG, "none");

        this.properties = properties;
        this.sign = "|KafkaStreamTable|" + topic + "|" + toHexString(hashCode());
        this.partitionsDetector = newSingleThreadScheduledExecutor(Threads.threadsNamed("partitions_detector" + sign));
        this.consumeFrom = consumeFrom;
        this.consumeTo = consumeTo;
        myHash = SystemProperty.getMyHash();
        serverCount = SystemProperty.getServerCount();
        stringColumns = new ArrayList<>(columns.size());
        types = new ArrayList<>(columns.size());
        timeColumnIndex = columns.indexOf(__time__);
        receiveTimeColumnIndex = columns.indexOf(__receive_time__);
        for (ByteArray column : columns) {
            String columnName = column.toString();
            stringColumns.add(columnName);
            types.add(columnTypeMap.get(columnName));
        }
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
        KafkaStreamTable kafkaStreamTable = this;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Consumer<Long, String> consumer = new KafkaConsumer<>(properties);
                consumer.assign(asList(topicPartition));
                if (null == offsetAndTimestamp) {
                    consumer.seekToBeginning(asList(topicPartition));
                } else {
                    consumer.seek(topicPartition, offsetAndTimestamp.offset());
                }

                Gson gson = new Gson();
                while (!Thread.interrupted()) {
                    try {
                        ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(sleepMs));
                        if (records.isEmpty()) {
                            continue;
                        }
                        TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
                        for (ConsumerRecord<Long, String> record : records) {
                            Long time = record.key();
                            if (-1 != consumeTo && time >= consumeTo) {
                                kafkaStreamTable.removePartition(topicPartition.partition());
                                return;
                            }

                            long now = System.currentTimeMillis();
                            Delay.DELAY.log("business-delay" + kafkaStreamTable.sign, time);
                            Delay.DELAY.log("data-interval" + kafkaStreamTable.sign, now);
                            Delay.RESIDENCE_TIME.log("data-residence-time" + kafkaStreamTable.sign, now - time);

                            String value = record.value();
                            JsonObject jsonObject = gson.fromJson(value, JsonObject.class);
                            for (int i = 0; i < stringColumns.size(); i++) {
                                if (i == timeColumnIndex) {
                                    tableBuilder.append(i, time);
                                } else if (i == receiveTimeColumnIndex) {
                                    tableBuilder.append(i, record.timestamp());
                                } else {
                                    JsonElement jsonElement = jsonObject.get(stringColumns.get(i));
                                    if (null == jsonElement || jsonElement.isJsonNull()) {
                                        tableBuilder.appendValue(i, null);
                                    } else {
                                        Type type = types.get(i);
                                        switch (type) {
                                            case DOUBLE:
                                                tableBuilder.append(i, jsonElement.getAsDouble());
                                                break;
                                            case BIGINT:
                                                tableBuilder.append(i, jsonElement.getAsLong());
                                                break;
                                            case INT:
                                                tableBuilder.append(i, jsonElement.getAsInt());
                                                break;
                                            case VARBYTE:
                                                tableBuilder.append(i, jsonElement.getAsString());
                                                break;
                                            default:
                                                throw new UnknownTypeException(type.name());
                                        }
                                    }
                                }
                            }
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

    protected synchronized void addPartition(int partition) {
        partitionSet.add(partition);
        partitionSetSize = partitionSet.size();
        lastUpdateMs = System.currentTimeMillis();
    }

    protected synchronized void removePartition(int partition) {
        partitionSet.remove(partition);
        partitionSetSize = partitionSet.size();
        lastUpdateMs = System.currentTimeMillis();
    }

    @Override
    public boolean isFinished() {
        if (-1 == consumeTo) {
            return false;
        }
        if (partitionSetSize <= 0 && System.currentTimeMillis() - lastUpdateMs >= finishDelayMs) {
            return true;
        }
        return false;
    }

    @Override
    public void start() {
        Consumer<String, String> consumer = newKafkaConsumer(properties);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        Map<TopicPartition, Long> topicPartitionTimes = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            topicPartitionTimes.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), consumeFrom);
        }
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsets = consumer.offsetsForTimes(topicPartitionTimes);
        for (TopicPartition topicPartition : topicPartitionOffsets.keySet()) {
            newConsumer(topicPartition, topicPartitionOffsets.get(topicPartition));
        }

        partitionsDetector.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                logger.info("{} partitions: {}", sign, myPartitions);
                Consumer<String, String> consumer = newKafkaConsumer(properties);
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                for (PartitionInfo partitionInfo : partitionInfos) {
                    newConsumer(new TopicPartition(topic, partitionInfo.partition()), null);
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        partitionsDetector.shutdownNow();
        for (Thread consumer : consumers) {
            consumer.interrupt();
        }
        consumers.clear();
        myPartitions.clear();
        partitionSet.clear();
        partitionSetSize = 0;
    }
}
