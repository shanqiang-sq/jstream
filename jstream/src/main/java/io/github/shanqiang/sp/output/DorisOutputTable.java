package io.github.shanqiang.sp.output;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.github.shanqiang.Threads;
import io.github.shanqiang.exception.UnknownTypeException;
import io.github.shanqiang.offheap.ByteArray;
import io.github.shanqiang.table.Column;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.Type;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.github.shanqiang.sp.StreamProcessing.handleException;
import static io.github.shanqiang.util.DateUtil.toDate;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DorisOutputTable extends AbstractOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(DorisOutputTable.class);
    private static final long oneHour = 3600 * 1000;

    private final String jdbcHostPorts;
    private final String loadAddress;
    private final String user;
    private final String password;
    private final String authHeader;
    private final String database;
    private final String tableName;
    private final String loadUrl;
    private final String[] distributedByColumns;
    private final int maxRetryTimes;
    private final int batchSize;
    private final boolean autoDropTable;
    protected final Map<String, Type> columnTypeMap;
    private final int columnCount;
    private final String jsonpaths;
    private final long flushInterval;

    private final long dataSpan;
    private final String partitionByTimeColumn;
    private final int buckets;
    private final int replicationNum;
    private final String storageMedium;
    private final ScheduledExecutorService autoAddDropPartition;
    private final ThreadPoolExecutor threadPoolExecutor;

    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });

    public DorisOutputTable(
            String jdbcHostPorts,
            String loadAddress,
            String user,
            String password,
            String database,
            String tableName,
            String[] distributedByColumns,
            String partitionByTimeColumn,
            int buckets,
            Map<String, Type> columnTypeMap) throws IOException {
        this(Runtime.getRuntime().availableProcessors(),
                jdbcHostPorts,
                loadAddress,
                user,
                password,
                database,
                tableName,
                distributedByColumns,
                partitionByTimeColumn,
                buckets,
                columnTypeMap);
    }

    public DorisOutputTable(
            int thread,
            String jdbcHostPorts,
            String loadAddress,
            String user,
            String password,
            String database,
            String tableName,
            String[] distributedByColumns,
            String partitionByTimeColumn,
            int buckets,
            Map<String, Type> columnTypeMap) throws IOException {
        this(thread,
                40000,
                Duration.ofSeconds(1),
                jdbcHostPorts,
                loadAddress,
                user,
                password,
                database,
                tableName,
                distributedByColumns,
                1,
                false,
                2,
                partitionByTimeColumn,
                buckets,
                1,
                "SSD",
                columnTypeMap);
    }

    /**
     * @param thread
     * @param batchSize
     * @param jdbcHostPorts        host1:port1,host2:port2...
     * @param loadAddress          host:port
     * @param user
     * @param password
     * @param database
     * @param tableName
     * @param distributedByColumns
     * @param maxRetryTimes
     * @param autoDropTable
     * @param columnTypeMap
     * @throws IOException
     */
    public DorisOutputTable(
            int thread,
            int batchSize,
            Duration flushInterval,
            String jdbcHostPorts,
            String loadAddress,
            String user,
            String password,
            String database,
            String tableName,
            String[] distributedByColumns,
            int maxRetryTimes,
            boolean autoDropTable,
            int dataSpanHours,
            String partitionByTimeColumn,
            int buckets,
            int replicationNum,
            String storageMedium,
            Map<String, Type> columnTypeMap) throws IOException {
        super(thread, "|DorisOutputTable|" + tableName);
        this.jdbcHostPorts = requireNonNull(jdbcHostPorts);
        this.loadAddress = requireNonNull(loadAddress);
        this.user = requireNonNull(user);
        this.password = requireNonNull(password);
        this.authHeader = basicAuthHeader(user, password);
        this.database = requireNonNull(database);
        this.tableName = requireNonNull(tableName);
        this.loadUrl = format("http://%s/api/%s/%s/_stream_load", loadAddress, database, tableName);
        this.distributedByColumns = requireNonNull(distributedByColumns);
        if (distributedByColumns.length < 1) {
            throw new IllegalArgumentException("at least one distributed by column");
        }
        this.maxRetryTimes = requireNonNull(maxRetryTimes);
        this.batchSize = batchSize;
        this.flushInterval = requireNonNull(flushInterval).toMillis();
        this.autoDropTable = autoDropTable;
        this.dataSpan = dataSpanHours * oneHour;
        this.partitionByTimeColumn = requireNonNull(partitionByTimeColumn);
        this.buckets = buckets;
        this.replicationNum = replicationNum;
        this.storageMedium = requireNonNull(storageMedium);
        this.columnTypeMap = requireNonNull(columnTypeMap);
        this.columnCount = columnTypeMap.size();

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < columnCount; i++) {
            sb.append('"');
            sb.append("$.c" + i);
            sb.append("\",");
        }
        sb.setLength(sb.length() - 1);
        sb.append(']');
        this.jsonpaths = sb.toString();

        this.autoAddDropPartition = newSingleThreadScheduledExecutor(Threads.threadsNamed("auto_add_drop_partition" + sign));
        threadPoolExecutor = new ThreadPoolExecutor(thread,
                thread,
                0,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1),
                new ThreadFactoryBuilder().setNameFormat("doris-output-%d").build());

        createTable();
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    private Connection connect() throws SQLException {
        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        //jdbc:mysql://[host1][:port1],[host2][:port2][,[host3][:port3]]...[/[database]][?propertyName1=propertyValue1[&propertyName2=propertyValue2]...]
        String jdbcUrl = format("jdbc:mysql://%s/%s", jdbcHostPorts, database);
        mysqlDataSource.setUrl(jdbcUrl);
        mysqlDataSource.setUser(user);
        mysqlDataSource.setPassword(password);
        mysqlDataSource.setAutoReconnect(true);

        return mysqlDataSource.getConnection();
    }

    private static String toDorisType(Type type) {
        switch (type) {
            case VARBYTE:
            case BIGDECIMAL:
                return "STRING";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case DOUBLE:
                return "DOUBLE";
            default:
                throw new UnknownTypeException(null == type ? "null" : type.name());
        }
    }

    private void createTable() throws IOException {
        StringBuilder fieldsBuilder = new StringBuilder();
        for (String columnName : columnTypeMap.keySet()) {
            Type type = columnTypeMap.get(columnName);
            fieldsBuilder.append("`").append(columnName).append("`").append(" ").append(toDorisType(type)).append(",");
        }

        String fieldsSchema = fieldsBuilder.toString();
        if (fieldsSchema.length() > 0) {
            fieldsSchema = fieldsSchema.substring(0, fieldsSchema.length() - 1);
        }

        /* 表不存在则创建表 */
        String createTableSql = format("CREATE TABLE IF NOT EXISTS %s (%s) ",
                tableName, fieldsSchema);
        createTableSql += format(" PARTITION BY RANGE (%s)" +
                        " () " +
                        " DISTRIBUTED BY HASH (%s) BUCKETS %d " +
                        " PROPERTIES(\"replication_num\" = \"%d\", " +
                        "\"in_memory\" = \"false\", " +
                        "\"storage_medium\" = \"%s\");",
                partitionByTimeColumn,
                String.join(",", distributedByColumns),
                buckets,
                replicationNum,
                storageMedium);

        logger.info(">>> create table sql: " + createTableSql);

        int retryCount = 0;
        while (retryCount < maxRetryTimes) {
            try {
                try (Connection connection = connect()) {
                    if (autoDropTable) {
                        PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE IF EXISTS " + tableName);
                        preparedStatement.execute();
                    }
                    PreparedStatement preparedStatement = connection.prepareStatement(createTableSql);
                    preparedStatement.execute();
                }
                return;
            } catch (Throwable t) {
                logger.error(">>> create table error: {}, has retried {} times", Throwables.getStackTraceAsString(t), retryCount);
                retryCount++;
                if (retryCount >= maxRetryTimes) {
                    throw new IOException(t);
                }
                try {
                    Thread.sleep(1 * 1000L);
                } catch (Throwable t2) {
                    logger.error("retry sleep error!", t2);
                }
            }
        }

        throw new IOException(">>> create doris table error for " + tableName + ", we have tried " + maxRetryTimes + " times");
    }

    private void dropPartitions() throws SQLException {
        long start = System.currentTimeMillis() - dataSpan;
        String partition = partitionName(start);

        String sql = format("SHOW PARTITIONS FROM %s ", tableName);
        try (Connection connection = connect()) {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    if ("PartitionName".equalsIgnoreCase(resultSetMetaData.getColumnName(i))) {
                        String partitionName = resultSet.getString(i);
                        if (partitionName.compareTo(partition) <= 0) {
                            dropPartition(partitionName);
                        }
                    }
                }
            }
        }
    }

    private void dropPartition(long start) throws SQLException {
        dropPartition(partitionName(start));
    }

    private void dropPartition(String partition) throws SQLException {
        String sql = format("ALTER TABLE %s DROP PARTITION IF EXISTS %s FORCE", tableName, partition);
        try (Connection connection = connect()) {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        }
    }

    private String partitionName(long ms) {
        return "p" + toDate(ms, "yyyyMMddHH");
    }

    private void addPartition(long start) throws SQLException {
        start = start / 1000 / 3600 * 3600 * 1000;
        long end = start + oneHour;
        String partition = partitionName(start);

        String sql = format("ALTER TABLE %s ADD PARTITION IF NOT EXISTS %s VALUES[(\"%d\"), (\"%d\"))",
                tableName, partition, start, end);

        try (Connection connection = connect()) {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();

            sql = format("ALTER TABLE %s MODIFY PARTITION %s SET (\"storage_medium\" = \"SSD\", \"storage_cooldown_time\" = \"9999-12-31 23:59:59\")",
                    tableName, partition);
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        }
    }

    @Override
    public void produce(Table table) throws InterruptedException {
        putTable(table);
    }

    private String escape(String src) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < src.length(); i++) {
            char c = src.charAt(i);
            if (c == '\\') {
                sb.append('\\');
                sb.append('\\');
            } else if (c == '"') {
                sb.append('\\');
                sb.append('"');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private int insert(CloseableHttpClient httpClient, List<Object> objectList, Gson gson) throws IOException {
        if (objectList.size() <= 0) {
            return 0;
        }
        if (objectList.size() % columnCount != 0) {
            throw new IllegalArgumentException(format("objectList.size: %d cannot divided by columnCount: %d",
                    objectList.size(),
                    columnCount));
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[{");
        int i = 0;
        for (Object object : objectList) {
            sb.append("\"c");
            sb.append(i);
            sb.append("\":");
            if (object instanceof BigDecimal) {
                object = object.toString();
            }
            if (object instanceof String || object instanceof ByteArray) {
                sb.append('"');
                sb.append(escape(object.toString()));
                sb.append('"');
            } else {
                sb.append(object);
            }

            i++;

            if (i == columnCount) {
                sb.append("},{");
                i = 0;
            } else {
                sb.append(",");
            }
        }
        sb.setLength(sb.length() - 2);
        sb.append(']');
        String content = sb.toString();

        HttpPut put = new HttpPut(loadUrl);
        StringEntity entity = new StringEntity(content, "UTF-8");
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        put.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        put.setHeader("format", "json");
        put.setHeader("strip_outer_array", "true");
        put.setHeader("jsonpaths", jsonpaths);
        put.setEntity(entity);

        try (CloseableHttpResponse response = httpClient.execute(put)) {
            String loadResult = "";
            if (response.getEntity() != null) {
                loadResult = EntityUtils.toString(response.getEntity());
            }
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                throw new IOException(
                        format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
            }

            JsonObject jsonObject = gson.fromJson(loadResult, JsonObject.class);
            if (!jsonObject.get("Status").getAsString().equals("Success")) {
                throw new IllegalStateException(loadResult);
            }
            return jsonObject.get("NumberLoadedRows").getAsInt();
        }
    }

    private void addPartitions() throws SQLException {
        long now = System.currentTimeMillis();
        addPartition(now);
        addPartition(now + oneHour);
    }

    @Override
    public void start() {
        try {
            addPartitions();
            dropPartitions();
        } catch (Throwable t) {
            logger.error("", t);
        }

        autoAddDropPartition.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    long now = System.currentTimeMillis();
                    addPartition(now);
                    addPartition(now + oneHour);

                    dropPartition(now - dataSpan);
                    dropPartition(now - dataSpan - oneHour);
                } catch (Throwable t) {
                    logger.error("", t);
                }
            }
        }, 0, 1, TimeUnit.MINUTES);

        for (int i = 0; i < thread; i++) {
            threadPoolExecutor.submit(new Runnable() {
                private long lastTime = 0;
                private List<Object> values = new ArrayList<>();
                private final Gson gson = new Gson();

                private void load(CloseableHttpClient httpClient) throws IOException {
                    insert(httpClient, values, gson);
                    values.clear();
                    lastTime = System.currentTimeMillis();
                }

                @Override
                public void run() {
                    try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
                        while (!Thread.interrupted()) {
                            try {
                                Table table = consume();
                                List<Column> columns = table.getColumns();

                                if (System.currentTimeMillis() - lastTime >= flushInterval && values.size() > 0) {
                                    load(httpClient);
                                }

                                for (int i = 0; i < table.size(); i++) {
                                    for (int j = 0; j < columns.size(); j++) {
                                        values.add(columns.get(j).get(i));
                                    }
                                    if (values.size() == batchSize * columns.size()) {
                                        load(httpClient);
                                    }
                                }

                            } catch (InterruptedException e) {
                                logger.info("interrupted");
                                break;
                            } catch (Throwable t) {
                                handleException(t);
                                break;
                            }
                        }
                        //线程结束前load一下还没有写入doris的数据
                        while (true) {
                            Table table = consume();
                            if (table.isEmpty()) {
                                load(httpClient);
                                break;
                            }
                            List<Column> columns = table.getColumns();

                            for (int i = 0; i < table.size(); i++) {
                                for (int j = 0; j < columns.size(); j++) {
                                    values.add(columns.get(j).get(i));
                                }
                            }
                        }
                    } catch (Throwable t) {
                        logger.error("", t);
                    }
                }
            });
        }
    }

    @Override
    public void stop() {
        threadPoolExecutor.shutdownNow();
        autoAddDropPartition.shutdownNow();
    }
}
