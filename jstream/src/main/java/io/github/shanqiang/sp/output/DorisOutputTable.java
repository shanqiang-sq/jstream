package io.github.shanqiang.sp.output;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.github.shanqiang.exception.UnknownTypeException;
import io.github.shanqiang.offheap.ByteArray;
import io.github.shanqiang.sp.QueueSizeLogger;
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
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.github.shanqiang.sp.StreamProcessing.handleException;
import static java.lang.Integer.parseInt;
import static java.lang.Integer.toHexString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DorisOutputTable extends AbstractOutputTable {
    private static final Logger logger = LoggerFactory.getLogger(DorisOutputTable.class);

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
    private final String sign;

    private final ThreadPoolExecutor threadPoolExecutor;
    private final QueueSizeLogger queueSizeLogger = new QueueSizeLogger();
    protected final QueueSizeLogger recordSizeLogger = new QueueSizeLogger();

    final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });

    public DorisOutputTable(String jdbcHostPorts,
                            String loadAddress,
                            String user,
                            String password,
                            String database,
                            String tableName,
                            String[] distributedByColumns,
                            Map<String, Type> columnTypeMap) throws IOException {
        this(Runtime.getRuntime().availableProcessors(),
                40000,
                jdbcHostPorts,
                loadAddress,
                user,
                password,
                database,
                tableName,
                distributedByColumns,
                1,
                false,
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
    public DorisOutputTable(int thread,
                            int batchSize,
                            String jdbcHostPorts,
                            String loadAddress,
                            String user,
                            String password,
                            String database,
                            String tableName,
                            String[] distributedByColumns,
                            int maxRetryTimes,
                            boolean autoDropTable,
                            Map<String, Type> columnTypeMap) throws IOException {
        super(thread);
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
        this.autoDropTable = autoDropTable;
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

        this.sign = "|DorisOutputTable|" + tableName + "|" + toHexString(hashCode());

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
//                return "STRING";
//                return "VARCHAR(65533)";
                return "VARCHAR(1000)";     //The size of a row cannot exceed the maximal row size: 100000
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
        createTableSql += format(" DISTRIBUTED BY HASH (%s) BUCKETS 64 " +
                        " PROPERTIES(\"replication_num\" = \"1\");",
                String.join(",", distributedByColumns));

        logger.info(">>> create table sql: " + createTableSql);

        int retryCount = 0;
        while (retryCount < maxRetryTimes) {
            try {
                Connection connection = connect();
                if (autoDropTable) {
                    PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE IF EXISTS " + tableName);
                    preparedStatement.execute();
                }
                PreparedStatement preparedStatement = connection.prepareStatement(createTableSql);
                preparedStatement.execute();

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

    @Override
    public void produce(Table table) throws InterruptedException {
        queueSizeLogger.logQueueSize("doris_output_queue_size" + sign, arrayBlockingQueueList);
        recordSizeLogger.logRecordSize("doris_output_queue_rows" + sign, arrayBlockingQueueList);
        putTable(table);
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
                sb.append(object.toString().replaceAll("\"", "\\\""));
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

    public void start() {
        for (int i = 0; i < thread; i++) {
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Gson gson = new Gson();
                    CloseableHttpClient httpClient = httpClientBuilder.build();

                    while (!Thread.interrupted()) {
                        try {
                            Table table = consume();
                            List<Column> columns = table.getColumns();

                            List<Object> values = new ArrayList<>();
                            for (int i = 0; i < table.size(); i++) {
                                for (int j = 0; j < columns.size(); j++) {
                                    values.add(columns.get(j).get(i));
                                }
                                if (values.size() == batchSize * columns.size()) {
                                    insert(httpClient, values, gson);
                                    values.clear();
                                }
                            }

                            if (values.size() > 0) {
                                insert(httpClient, values, gson);
                            }
                        } catch (InterruptedException e) {
                            logger.info("interrupted");
                            break;
                        } catch (Throwable t) {
                            handleException(t);
                            break;
                        }
                    }
                }
            });
        }
    }

    @Override
    public void stop() {
        threadPoolExecutor.shutdownNow();
    }
}
