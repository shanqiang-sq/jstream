package io.github.shanqiang.sp.input;

import io.github.shanqiang.SystemProperty;
import io.github.shanqiang.offheap.ByteArray;
import io.github.shanqiang.sp.Delay;
import io.github.shanqiang.table.Column;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import io.github.shanqiang.util.DateUtil;
import io.github.shanqiang.util.IpUtil;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.ListShardRequest;
import com.aliyun.openservices.log.response.ListConsumerGroupResponse;
import com.aliyun.openservices.log.response.ListShardResponse;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import io.github.shanqiang.sp.StreamProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.github.shanqiang.sp.QueueSizeLogger.addQueueSizeLog;
import static io.github.shanqiang.sp.QueueSizeLogger.addRecordSizeLog;
import static java.lang.Integer.toHexString;
import static java.util.Objects.requireNonNull;

public class SlsStreamTable extends AbstractStreamTable {
    private static final Logger logger = LoggerFactory.getLogger(SlsStreamTable.class);

    private final String endPoint;
    private final String accessId;
    private final String accessKey;
    private final String project;
    private final String logstore;
    private final String consumerGroup;
    private final int consumeFrom;
    private final int consumeTo;
    private long finishDelayMs = 30000;
    private long lastUpdateMs = System.currentTimeMillis();
    private final Set<Integer> shardSet = new HashSet<>();
    //??????shardSet.size()????????????????????????????????????????????????shardSetSize????????????
    private int shardSetSize;

    private final List<ClientWorker> workers;

    public SlsStreamTable(String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          Map<String, Type> columnTypeMap) {
        this(1, endPoint, accessId, accessKey, project, logstore, consumerGroup, columnTypeMap);
    }

    public SlsStreamTable(int thread,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          Map<String, Type> columnTypeMap) {
        this(thread, endPoint, accessId, accessKey, project, logstore, consumerGroup,
                (int) (System.currentTimeMillis() / 1000),
                -1,
                columnTypeMap);
    }

    /**
     *
     * @param thread                thread number
     * @param endPoint              end point like: http://cn-shanghai-corp.sls.aliyuncs.com
     * @param accessId              ak
     * @param accessKey             sk
     * @param project               project
     * @param logstore              logstore
     * @param consumerGroup         unique consumer group name for this logstore
     * @param consumeFrom           example: "2021-04-09 16:16:00"
     * @param columnTypeMap         stream table's columns and their types build by ColumnTypeBuilder
     * @throws ParseException       if consumeFrom can not be parsed to a valid datetime this exception will be thrown
     */
    public SlsStreamTable(int thread,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          String consumeFrom,
                          Map<String, Type> columnTypeMap) throws ParseException {
        this(thread, endPoint, accessId, accessKey, project, logstore, consumerGroup, (int) (DateUtil.parseDate(consumeFrom) / 1000), -1, columnTypeMap);
    }

    public SlsStreamTable(int thread,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          String consumeFrom,
                          String consumeTo,
                          Map<String, Type> columnTypeMap) throws ParseException {
        this(thread, endPoint, accessId, accessKey, project, logstore, consumerGroup,
                (int) (DateUtil.parseDate(consumeFrom) / 1000),
                (int) (DateUtil.parseDate(consumeTo) / 1000),
                columnTypeMap);
    }

    public SlsStreamTable(int thread,
                          String endPoint,
                          String accessId,
                          String accessKey,
                          String project,
                          String logstore,
                          String consumerGroup,
                          int consumeFrom,
                          int consumeTo,
                          Map<String, Type> columnTypeMap) {
        super(thread, columnTypeMap, "|SlsStreamTable|" + project + "|" + logstore);

        this.endPoint = requireNonNull(endPoint);
        this.accessId = requireNonNull(accessId);
        this.accessKey = requireNonNull(accessKey);
        this.project = requireNonNull(project);
        this.logstore = requireNonNull(logstore);
        this.consumerGroup = requireNonNull(consumerGroup);
        this.consumeFrom = consumeFrom;
        this.consumeTo = consumeTo;

        workers = new ArrayList<>(thread);
    }

    /**
     * use synchronized to assure the visibility of this.finishDelayMs in other thread
     * when all shard have consumed to consumeTo time, maybe SLS is happening auto split shard wait a delay time to assure
     * finish consume the pre-split-shard and two new split shard to consumeTo time
     * @param finishDelay default: 30 seconds
     */
    public synchronized void setFinishDelaySeconds(Duration finishDelay) {
        this.finishDelayMs = finishDelay.toMillis();
    }

    private synchronized void addShard(int shardId) {
        shardSet.add(shardId);
        shardSetSize = shardSet.size();
        lastUpdateMs = System.currentTimeMillis();
    }

    private synchronized void removeShard(int shardId) {
        shardSet.remove(shardId);
        shardSetSize = shardSet.size();
        lastUpdateMs = System.currentTimeMillis();
    }

    private class LogHubProcessor implements ILogHubProcessor {
        private final int threadId;
        private final SlsStreamTable slsStreamTable;

        private int shardId;
        // last save checkpoint time
        private long mLastCheckTime = 0;

        LogHubProcessor(int threadId, SlsStreamTable slsStreamTable) {
            this.threadId = threadId;
            this.slsStreamTable = slsStreamTable;
        }

        @Override
        public void initialize(int shardId) {
            this.shardId = shardId;
            slsStreamTable.addShard(shardId);
        }

        // ??????????????????????????????????????????????????????????????????????????????????????????
        @Override
        public String process(List<LogGroupData> logGroups, ILogHubCheckPointTracker checkPointTracker) {
            try {
                if (!slsStreamTable.shardSet.contains(shardId)) {
                    return null;
                }
                TableBuilder tableBuilder = new TableBuilder(columnTypeMap);
                Map<ByteArray, ByteArray> tmp = new HashMap<>();
                for (LogGroupData logGroup : logGroups) {
                    new SlsParser(logGroup, new SlsParser.Callback() {
                        private int size = 0;

                        @Override
                        public void keyValue(byte[] rawBytes, int keyOffset, int keyLength, int valueOffset, int valueLength) {
                            if (-1 == keyOffset || -1 == valueOffset) {
                                return;
                            }

                            ByteArray key = new ByteArray(rawBytes, keyOffset, keyLength);

                            //?????????????????????????????????????????????
//                            if (!columnNames.contains(key)) {
//                                return;
//                            }

                            tmp.put(key, new ByteArray(rawBytes, valueOffset, valueLength));
                        }

                        @Override
                        public void nextLog(int time) {
                            if (tmp.isEmpty()) {
                                return;
                            }
                            if (-1 != consumeTo && time >= consumeTo) {
                                slsStreamTable.removeShard(shardId);
                                return;
                            }

                            long now = System.currentTimeMillis();
                            long ms = (long) time * 1000;
                            Delay.DELAY.log("business-delay" + slsStreamTable.sign, ms);
                            Delay.DELAY.log("data-interval" + slsStreamTable.sign, now);
                            Delay.RESIDENCE_TIME.log("data-residence-time" + slsStreamTable.sign, now - ms);

                            int i = 0;
                            for (ByteArray key : columns) {
                                if (key == __machine_uuid__ ||
                                        key == __category__ ||
                                        key == __source__ ||
                                        key == __topic__) {
                                    i++;
                                    continue;
                                }
                                if (key == __time__) {
                                    tableBuilder.append(i++, ms);
                                } else {
                                    tableBuilder.append(i++, tmp.get(key));
                                }
                            }
                            size++;
                            tmp.clear();
                        }

                        private void addExtraColumn(ByteArray columnName, ByteArray value) {
                            if (columnNames.contains(columnName)) {
                                Column column = tableBuilder.getColumn(columnName.toString());
                                for (int i = 0; i < size; i++) {
                                    column.add(value);
                                }
                            }
                        }

                        @Override
                        public void end(ByteArray category, ByteArray topic, ByteArray source, ByteArray machineUUID) {
                            addExtraColumn(__category__, category);
                            addExtraColumn(__topic__, topic);
                            addExtraColumn(__source__, source);
                            addExtraColumn(__machine_uuid__, machineUUID);
                        }
                    });
                }

                arrayBlockingQueueList.get(threadId).put(tableBuilder.build());

                return null;
            } catch (Throwable t) {
                StreamProcessing.handleException(t);
                return null;
            } finally {
                //????????????????????????finally???????????????????????????????????????????????????saveCheckPoint???????????????????????????????????????shard??????????????????shard
                //???????????????check point??????????????????shard???????????????????????????????????????????????????shard????????????
                long curTime = System.currentTimeMillis();
                // ??????90???????????????Checkpoint?????????????????????90????????????Worker???????????????????????????Worker???????????????Checkpoint?????????????????????????????????????????????????????????
                if (curTime - mLastCheckTime > 90 * 1000) {
                    try {
                        //?????????true???????????????Checkpoint?????????????????????false?????????Checkpoint??????????????????????????????60?????????Checkpoint?????????????????????
                        checkPointTracker.saveCheckPoint(true);
                    } catch (Throwable t) {
                        StreamProcessing.handleException(t);
                    }
                    mLastCheckTime = curTime;
                }
            }
        }

        // ???Worker????????????????????????????????????????????????????????????????????????
        @Override
        public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
            //???Checkpoint???????????????????????????
            try {
                slsStreamTable.removeShard(shardId);
                checkPointTracker.saveCheckPoint(true);
            } catch (Throwable t) {
                StreamProcessing.handleException(t);
            }
        }
    }

    private class LogHubProcessorFactory implements ILogHubProcessorFactory {
        private final int threadId;
        private final SlsStreamTable slsStreamTable;

        LogHubProcessorFactory(int threadId, SlsStreamTable slsStreamTable) {
            this.threadId = threadId;
            this.slsStreamTable = slsStreamTable;
        }

        @Override
        public ILogHubProcessor generatorProcessor() {
            // ???????????????????????????
            return new LogHubProcessor(threadId, slsStreamTable);
        }
    }

    private void updateCheckpoint(Client client, long timestamp) throws LogException, InterruptedException {
        try {
//        long timestamp = Timestamp.valueOf("2017-11-15 00:00:00").getTime() / 1000;
            ListShardResponse response = client.ListShard(new ListShardRequest(project, logstore));
            for (Shard shard : response.GetShards()) {
                int shardId = shard.GetShardId();
                String cursor = client.GetCursor(project, logstore, shardId, timestamp).GetCursor();
                client.UpdateCheckPoint(project, logstore, consumerGroup, shardId, cursor);
                logger.info("update shardId: {}, cursor: {}", shardId, cursor);
            }
        } catch (LogException e) {
            if (!"shard not exist".equals(e.getMessage())) {
                throw e;
            }
            // ???????????????consumer group?????????updateCheckPoint??????shard not exist??????????????????????????????SLS????????????consumer group??????
            // shard????????????????????????
            Thread.sleep(1000);
            updateCheckpoint(client, timestamp);
        }
    }

    private ConsumerGroup getConsumerGroup(Client slsClient) throws LogException {
        ListConsumerGroupResponse response;
        response = slsClient.ListConsumerGroup(this.project, this.logstore);
        if (response != null) {
            Iterator iterator = response.GetConsumerGroups().iterator();

            while (iterator.hasNext()) {
                ConsumerGroup item = (ConsumerGroup) iterator.next();
                if (item.getConsumerGroupName().equalsIgnoreCase(this.consumerGroup)) {
                    return item;
                }
            }
        }

        return null;
    }

    @Override
    public void start() {
        try {
            long heartbeatIntervalMillis = 5000;
            //??????10???????????????????????????????????????consumer???????????????consumer????????????
            int timeout = (int) (heartbeatIntervalMillis * 10 / 1000);
            //????????????????????????????????????watermark???????????????
            boolean consumeInOrder = true;
            Client slsClient = new Client(endPoint, accessId, accessKey);
            if (null == getConsumerGroup(slsClient)) {
                slsClient.CreateConsumerGroup(project, logstore, new ConsumerGroup(consumerGroup, timeout, consumeInOrder));
            }

            updateCheckpoint(slsClient, consumeFrom);

            for (int i = 0; i < thread; i++) {
                // consumer_??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????Logstore???
                // ???????????????????????????????????????IP??????????????????maxFetchLogGroupSize??????????????????????????????LogGroup???????????????????????????????????????
                // ?????????????????????????????????(0,1000]???
                LogHubConfig config = new LogHubConfig(consumerGroup,
                        "consumer_" + IpUtil.getIp() + "_" + SystemProperty.mySign() + "_" + i,
                        endPoint,
                        project,
                        logstore,
                        accessId,
                        accessKey,
                        consumeFrom);
                config.setHeartBeatIntervalMillis(heartbeatIntervalMillis);
                config.setTimeoutInSeconds(timeout);
                config.setConsumeInOrder(consumeInOrder);
                config.setMaxFetchLogGroupSize(1000);
                ClientWorker worker = new ClientWorker(new LogHubProcessorFactory(i, this), config);

                workers.add(worker);
                Thread thread = new Thread(worker, "sls-consumer-" + i);
                //Thread???????????????ClientWorker??????????????????ClientWorker?????????Runnable?????????
                thread.start();
            }
        } catch (LogException | InterruptedException | LogHubClientWorkerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isFinished() {
        if (-1 == consumeTo) {
            return false;
        }
        if (shardSetSize <= 0 && System.currentTimeMillis() - lastUpdateMs >= finishDelayMs) {
            return true;
        }
        return false;
    }

    @Override
    public void stop() {
        try {
            for (ClientWorker worker : workers) {
                //??????Worker???Shutdown??????????????????????????????????????????????????????????????????
                worker.shutdown();
            }

            //ClientWorker????????????????????????????????????????????????Shutdown????????????????????????????????????????????????????????????sleep?????????30??????
            Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
            logger.info("interrupted");
        }
    }
}
