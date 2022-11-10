package io.github.shanqiang.sp.output;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import io.github.shanqiang.Threads;
import io.github.shanqiang.exception.UnknownTypeException;
import io.github.shanqiang.sp.StreamProcessing;
import io.github.shanqiang.table.Column;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.github.shanqiang.util.ScalarUtil.toDouble;
import static io.github.shanqiang.util.ScalarUtil.toInteger;
import static io.github.shanqiang.util.ScalarUtil.toLong;
import static io.github.shanqiang.util.ScalarUtil.toStr;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ClickhouseOutputTable extends AbstractOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseOutputTable.class);

    private final String jdbcUrl;
    private final String httpPort;
    private final String tableName;
    private final String userName;
    private final String password;
    private final String clusterName;
    private final String databaseName;
    private final int maxRetryTimes;
    private final int multiple;
    private final int batchSize;
    private final long flushInterval;
    private final String ttl_expr;
    protected final Map<String, Type> columnTypeMap;
    private final String insertPrefix;
    private final String engine;
    private final boolean replica;
    private final String storagePolicy;
    private final String[] indexColumns;
    private final String timeColumn;
    private final ScheduledExecutorService partitionsDetector;
    private Map<Integer, String[]> shardMap = new ConcurrentHashMap<>();
    private final Map<Integer, List<Thread>> threadMap = new ConcurrentHashMap<>();

    public ClickhouseOutputTable(
            int multiple,
            String jdbcUrl,
            String userName,
            String password,
            String clusterName,
            String databaseName,
            String tableName,
            String[] indexColumns,
            String timeColumn,
            Map<String, Type> columnTypeMap) throws IOException {
        this(
                multiple,
                40000,
                Duration.ofSeconds(10),
                jdbcUrl,
                userName,
                password,
                clusterName,
                databaseName,
                tableName,
                indexColumns,
                timeColumn,
                1,
                "30 day",
                "ReplicatedMergeTree",
                "ssd_hdd",
                true,
                columnTypeMap
        );
    }

    /**
     * @param multiple      开shard数的multiple倍的线程每个线程一个连接写Clickhouse
     * @param batchSize
     * @param jdbcUrl
     * @param userName
     * @param password
     * @param clusterName
     * @param databaseName
     * @param tableName
     * @param indexColumns
     * @param timeColumn
     * @param maxRetryTimes
     * @param ttl_expr
     * @param engine
     * @param storagePolicy
     * @param replica
     * @param columnTypeMap
     * @throws IOException
     */
    public ClickhouseOutputTable(
            int multiple,
            int batchSize,
            Duration flushInterval,
            String jdbcUrl,
            String userName,
            String password,
            String clusterName,
            String databaseName,
            String tableName,
            String[] indexColumns,
            String timeColumn,
            int maxRetryTimes,
            String ttl_expr,
            String engine,
            String storagePolicy,
            boolean replica,
            Map<String, Type> columnTypeMap) throws IOException {
        super(10, "|ClickhouseOutputTable|" + tableName);
        this.multiple = multiple;
        this.jdbcUrl = requireNonNull(jdbcUrl);
        this.httpPort = jdbcUrl.split(",", 2)[0].split(":", 2)[1];
        this.userName = requireNonNull(userName);
        this.password = requireNonNull(password);
        this.clusterName = requireNonNull(clusterName);
        this.databaseName = requireNonNull(databaseName);
        this.tableName = requireNonNull(tableName);
        this.indexColumns = indexColumns;
        this.timeColumn = timeColumn;
        this.maxRetryTimes = maxRetryTimes;
        this.batchSize = batchSize;
        this.flushInterval = requireNonNull(flushInterval).toMillis();
        this.ttl_expr = ttl_expr;
        this.engine = engine;
        this.storagePolicy = storagePolicy;
        this.replica = replica;
        this.columnTypeMap = requireNonNull(columnTypeMap);
        this.insertPrefix = "insert into " + databaseName + "." + tableName;
        this.partitionsDetector = newSingleThreadScheduledExecutor(Threads.threadsNamed("shards_detector"));

        createTable();
    }

    private ClickHouseConnection connect() throws SQLException {
        String connString = format("jdbc:ch://%s/%s?health_check_interval=5000&load_balancing_policy=roundRobin&failover=2",
                jdbcUrl, databaseName);
        ClickHouseDataSource ds = new ClickHouseDataSource(connString);
        ClickHouseConnection conn = ds.getConnection(userName, password);

        return conn;
    }

    private ClickHouseConnection connect(String ip) throws SQLException {
        String connString = format("jdbc:ch://%s:%s/%s?health_check_interval=5000&failover=2", ip, httpPort, databaseName);
        ClickHouseDataSource ds = new ClickHouseDataSource(connString);
        ClickHouseConnection conn = ds.getConnection(userName, password);

        return conn;
    }

    private static String toDorisType(Type type) {
        switch (type) {
            case VARBYTE:
            case BIGDECIMAL:
                return "String";
            case INT:
                return "Int32";
            case BIGINT:
                return "Int64";
            case DOUBLE:
                return "Double";
            default:
                throw new UnknownTypeException(null == type ? "null" : type.name());
        }
    }

    private void createTable() {
        StringBuilder fieldsBuilder = new StringBuilder();
        for (String columnName : columnTypeMap.keySet()) {
            Type type = columnTypeMap.get(columnName);
            if (columnName.equals(timeColumn)) {
                //时间列用作ttl设置,需要设置成clickhouse中的Datetime类型
                fieldsBuilder.append("`").append(columnName).append("`").append(" ").append("Datetime").append(",");
            } else {
                fieldsBuilder.append("`").append(columnName).append("`").append(" ").append(toDorisType(type)).append(",");
            }
        }

        String fieldsSchema = fieldsBuilder.toString();
        if (fieldsSchema.length() > 0) {
            fieldsSchema = fieldsSchema.substring(0, fieldsSchema.length() - 1);
        }
        String createDatabaseSql = format("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER %s", databaseName, clusterName);

        /* 表不存在则创建表 */
        String createLocalTableSql = format("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (%s) ",
                tableName, clusterName, fieldsSchema);
        if (replica == true) {
            //有副本的集群需要设置副本配置,配置会保存在zookeeper
            createLocalTableSql += format("ENGINE = %s('/clickhouse/tables/{shard}/%s/%s', '{replica}') " +
                            "ORDER BY (%s) " +
                            "TTL %s + INTERVAL %s " +
                            "SETTINGS index_granularity = 8192",
                    engine,
                    databaseName,
                    tableName,
                    String.join(",", indexColumns),
                    timeColumn,
                    ttl_expr,
                    storagePolicy);
        } else {
            //没有副本的情况建表语句
            createLocalTableSql += format("ENGINE = %s " +
                            "ORDER BY (%s) " +
                            "TTL %s + INTERVAL %s " +
                            "SETTINGS index_granularity = 8192",
                    engine,
                    String.join(",", indexColumns),
                    timeColumn,
                    ttl_expr,
                    storagePolicy);
        }
        if (storagePolicy != null || storagePolicy.equals("")) {
            createLocalTableSql += format(", storage_policy = '%s'", storagePolicy);
        }
        logger.info(">>> create local table sql: " + createLocalTableSql);

        String createDistributedTableSql = format("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (%s) " +
                        "ENGINE = Distributed('%s', '%s', '%s', rand())",
                tableName + "_all",
                clusterName,
                fieldsSchema,
                clusterName,
                databaseName,
                tableName
        );
        logger.info(">>> create distributed table sql: " + createDistributedTableSql);

        int retryCount = 0;
        while (retryCount < maxRetryTimes) {
            try {
                try (ClickHouseConnection connection = connect()) {
                    PreparedStatement preparedStatement = connection.prepareStatement(createDatabaseSql);
                    preparedStatement.execute();
                    preparedStatement = connection.prepareStatement(createLocalTableSql);
                    preparedStatement.execute();
                    preparedStatement = connection.prepareStatement(createDistributedTableSql);
                    preparedStatement.execute();
                }
                return;
            } catch (Throwable t) {
                logger.error(">>> create table error: {}, has retried {} times", getStackTraceAsString(t), retryCount);
                retryCount++;
                if (retryCount >= maxRetryTimes) {
                    throw new IllegalStateException(t);
                }
                try {
                    Thread.sleep(1 * 1000L);
                } catch (Throwable t2) {
                    logger.error("retry sleep error!", t2);
                }
            }
        }

        throw new IllegalStateException(">>> create clickhouse table error for " + tableName + ", we have tried " + maxRetryTimes + " times");
    }

    private void setValues(PreparedStatement preparedStatement, List<Object> objectList) throws SQLException {
        for (int i = 0; i < objectList.size(); ) {
            int col = 1;
            for (Type type : columnTypeMap.values()) {
                Object object = objectList.get(i);
                switch (type) {
                    case VARBYTE:
                        if (null == object) {
                            preparedStatement.setString(col, "");
                            break;
                        }
                        preparedStatement.setString(col, toStr(objectList.get(i)));
                        break;
                    case INT:
                        if (null == object) {
                            preparedStatement.setInt(col, 0);
                            break;
                        }
                        preparedStatement.setInt(col, toInteger(objectList.get(i)));
                        break;
                    case BIGINT:
                        if (null == object) {
                            preparedStatement.setLong(col, 0);
                            break;
                        }
                        preparedStatement.setLong(col, toLong(objectList.get(i)));
                        break;
                    case DOUBLE:
                        if (null == object) {
                            preparedStatement.setDouble(col, 0);
                            break;
                        }
                        preparedStatement.setDouble(col, toDouble(objectList.get(i)));
                        break;
                    default:
                        throw new UnknownTypeException(type.name());
                }
                col++;
                i++;
            }
            preparedStatement.addBatch();
        }
    }

    private void insert(PreparedStatement batchPreparedStatement, List<Object> objectList) throws SQLException {
        if (objectList.size() <= 0) {
            return;
        }
        setValues(batchPreparedStatement, objectList);
        batchPreparedStatement.executeBatch();
        batchPreparedStatement.clearBatch();
    }

    private PreparedStatement prepareStatement(ClickHouseConnection connection, int size) {
        if (size < 1) {
            throw new IllegalArgumentException();
        }
        if (size % columnTypeMap.size() != 0) {
            throw new IllegalArgumentException(format("size: %d", size));
        }
        try {
            return connection.prepareStatement(insertPrefix);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void detectShards() {
        try (ClickHouseConnection conn = connect()) {
            String sql = format("SELECT shard_num, groupArray(host_address) AS list, arrayStringConcat(list, ',')" +
                            "FROM system.clusters " +
                            "WHERE cluster = '%s' " +
                            "GROUP BY shard_num " +
                            "ORDER BY shard_num",
                    clusterName);
            ConcurrentHashMap<Integer, String[]> tempShard = new ConcurrentHashMap();
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        int shardNum = rs.getInt(1);
                        String shardIPString = rs.getString(3);
                        String[] shardIPList = shardIPString.split(",");
                        tempShard.put(shardNum, shardIPList);
                    }
                    if (tempShard.size() > 0) {
                        shardMap = tempShard;
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void start() {
        partitionsDetector.scheduleWithFixedDelay(() -> {
            detectShards();
            logger.info("clickhouse shards: {}", shardMap);
            //如果有新的shard,启动新的线程去执行写入任务
            if (shardMap.size() != 0) {
                shardMap.forEach((num, ipList) -> {
                    if (!threadMap.containsKey(num)) {
                        //如果有新的shard加入,需要在新的shard上自动建表
                        createTable();
                        if (ipList.length <= 0) {
                            throw new IllegalStateException("ipList is empty");
                        }
                        for (int i = 0; i < multiple; i++) {
                            Thread thread = new Thread(new InsertThread(num, ipList), format("shard num : %s-%s", num.toString(), ipList[0]));
                            List<Thread> threadList = threadMap.get(num);
                            if (null == threadList) {
                                threadList = new ArrayList<>(multiple);
                                threadMap.put(num, threadList);
                            }
                            threadList.add(thread);
                            thread.start();
                        }
                    }
                });

                List<Integer> willRemove = new ArrayList<>();
                threadMap.forEach((num, threadList) -> {
                    //如果shard不存在了,将线程终止
                    if (!shardMap.containsKey(num)) {
                        for (Thread thread : threadList) {
                            thread.interrupt();
                        }
                        willRemove.add(num);
                    }
                });
                for (Integer num : willRemove) {
                    threadMap.remove(num);
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        threadMap.forEach((num, threadList) -> {
            for (Thread thread : threadList) {
                thread.interrupt();
            }
        });
        threadMap.clear();
        shardMap.clear();
    }

    @Override
    public void produce(Table table) throws InterruptedException {
        putTable(table);
    }

    public class InsertThread implements Runnable {
        private long lastTime = 0;
        private final List<Object> values = new ArrayList<>();
        private final int shard;
        private final String[] ipList;

        public InsertThread(int shard, String[] ipList) {
            this.shard = shard;
            this.ipList = ipList;
        }

        private void load(PreparedStatement batchPreparedStatement) throws SQLException {
            long startTime = System.currentTimeMillis();
            insert(batchPreparedStatement, values);
            lastTime = System.currentTimeMillis();
            logger.info("clickhouse load size {} cost {}ms", values.size() / columnTypeMap.size(), lastTime - startTime);
            values.clear();
        }

        ClickHouseConnection connectAlive() {
            for (String ip : ipList) {
                try {
                    return connect(ip);
                } catch (Throwable t) {
                    logger.error("", t);
                }
            }
            throw new IllegalStateException("no alived instance");
        }

        @Override
        public void run() {
            ClickHouseConnection connection = connectAlive();
            PreparedStatement batchPreparedStatement = prepareStatement(connection, batchSize * columnTypeMap.size());
            while (!Thread.interrupted()) {
                try {
                    Table table = consume();
                    List<Column> columns = table.getColumns();

                    if (System.currentTimeMillis() - lastTime > flushInterval && values.size() > 0) {
                        load(batchPreparedStatement);
                    }

                    for (int i = 0; i < table.size(); i++) {
                        for (int j = 0; j < columns.size(); j++) {
                            values.add(columns.get(j).get(i));
                        }
                        if (values.size() == batchSize * columns.size()) {
                            load(batchPreparedStatement);
                        }
                    }
                } catch (InterruptedException e) {
                    logger.info("interrupted");
                    break;
                } catch (Throwable t) {
                    StreamProcessing.handleException(t);
                    break;
                }
            }
            //线程结束前load一下还没有写入clickhouse的数据
            while (true) {
                try {
                    Table table = consume();
                    if (table.isEmpty()) {
                        load(batchPreparedStatement);
                        break;
                    }
                    List<Column> columns = table.getColumns();
                    for (int i = 0; i < table.size(); i++) {
                        for (int j = 0; j < columns.size(); j++) {
                            values.add(columns.get(j).get(i));
                        }
                    }
                } catch (Throwable t) {
                    throw new IllegalStateException(t);
                }
            }
        }
    }
}
