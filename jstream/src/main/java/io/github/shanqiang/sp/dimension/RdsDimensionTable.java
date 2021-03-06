package io.github.shanqiang.sp.dimension;

import io.github.shanqiang.exception.InconsistentColumnSizeException;
import io.github.shanqiang.exception.UnknownTypeException;
import io.github.shanqiang.table.Index;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.table.TableBuilder;
import io.github.shanqiang.table.Type;
import io.github.shanqiang.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class RdsDimensionTable extends DimensionTable {
    private static final Logger logger = LoggerFactory.getLogger(RdsDimensionTable.class);

    protected final String url;
    protected final String userName;
    protected final String password;
    private final Duration refreshInterval;
    private final Map<String, Type> columnTypeMap;
    private final String[] primaryKeyColumnNames;
    private final String sql;
    private final String myName;

    public RdsDimensionTable(String jdbcUrl,
                             String tableName,
                             final String userName,
                             final String password,
                             Duration refreshInterval,
                             Map<String, Type> columnTypeMap,
                             String... primaryKeyColumnNames) {
        this(jdbcUrl, userName, password, refreshInterval,
                format("select %s from %s", String.join(",", columnTypeMap.keySet()), tableName),
                columnTypeMap,
                primaryKeyColumnNames);
    }

    abstract protected DataSource newDataSource();

    public RdsDimensionTable(String jdbcUrl,
                             final String userName,
                             final String password,
                             Duration refreshInterval,
                             final String sql,
                             Map<String, Type> columnTypeMap,
                             String... primaryKeyColumnNames) {
        this.url = requireNonNull(jdbcUrl);
        this.userName = requireNonNull(userName);
        this.password = requireNonNull(password);
        this.refreshInterval = requireNonNull(refreshInterval);
        this.columnTypeMap = requireNonNull(columnTypeMap);
        if (columnTypeMap.size() < 1) {
            throw new IllegalArgumentException();
        }
        this.primaryKeyColumnNames = requireNonNull(primaryKeyColumnNames);
        if (primaryKeyColumnNames.length < 1) {
            throw new IllegalArgumentException();
        }

        this.sql = requireNonNull(sql);
        this.myName = format("%s: %s %s", this.getClass().getSimpleName(), url, sql.substring(0, sql.length() > 20 ? 20 : sql.length()));

        final DimensionTable that = this;
        new ScheduledThreadPoolExecutor(1, Threads.threadsNamed(myName)).
                scheduleWithFixedDelay(new Runnable() {
                    @Override
                    public void run() {
                        DataSource dataSource = newDataSource();
                        try (
                             Connection connection = dataSource.getConnection();
                             PreparedStatement preparedStatement = connection.prepareStatement(sql);
                             ResultSet resultSet = preparedStatement.executeQuery()
                        ) {
                            long pre = System.currentTimeMillis();
                            logger.info("begin to load {}", myName);
                            TableBuilder tableBuilder = new TableBuilder(columnTypeMap);

                            if (resultSet.getMetaData().getColumnCount() != columnTypeMap.size()) {
                                throw new InconsistentColumnSizeException();
                            }
                            int row = 0;
                            while (resultSet.next()) {
                                if (debug(row)) {
                                    break;
                                }

                                int i = 0;
                                for (Type type : columnTypeMap.values()) {
                                    int i1 = i + 1;
                                    switch (type) {
                                        case INT:
                                            tableBuilder.append(i, resultSet.getInt(i1));
                                            break;
                                        case BIGINT:
                                            tableBuilder.append(i, resultSet.getLong(i1));
                                            break;
                                        case DOUBLE:
                                            tableBuilder.append(i, resultSet.getDouble(i1));
                                            break;
                                        case VARBYTE:
                                            tableBuilder.append(i, resultSet.getString(i1));
                                            break;
                                        default:
                                            throw new UnknownTypeException(type.name());
                                    }
                                    i++;
                                }
                                row++;

                                long now = System.currentTimeMillis();
                                if (now - pre > 5000) {
                                    logger.info("{} have loaded {} rows", myName, row);
                                    pre = now;
                                }
                            }

                            Table table = tableBuilder.build();
                            Index index = table.createIndex(primaryKeyColumnNames);
                            tableIndex = new TableIndex(table, index);
                            callback(that);
                            logger.info("end to load {}, rows: {}, index.size: {}", myName, row, index.getColumns2Rows().size());
                        } catch (Throwable t) {
                            logger.error("", t);
                            try {
                                Thread.sleep(10_000);
                                run();
                            } catch (Throwable t1) {
                                logger.error("", t1);
                            }
                        }
                    }
                }, 0, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
    }
}
