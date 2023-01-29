package io.github.shanqiang.sp.dimension;

import com.shaded.mysql5.jdbc.jdbc2.optional.MysqlDataSource;
import io.github.shanqiang.table.Type;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Map;

public class MysqlDimensionTable5 extends RdsDimensionTable {
    public MysqlDimensionTable5(String jdbcUrl,
                                String tableName,
                                String userName,
                                String password,
                                Duration refreshInterval,
                                Map<String, Type> columnTypeMap,
                                String... primaryKeyColumnNames) {
        super(jdbcUrl, tableName, userName, password, refreshInterval, columnTypeMap, primaryKeyColumnNames);
    }

    /**
     *
     * @param jdbcUrl                   jdbc url like: jdbc:mysql://localhost:3306/e-commerce
     * @param userName                  username
     * @param password                  password
     * @param refreshInterval           refresh interval
     * @param sql                       sql to select from mysql
     * @param columnTypeMap             dimension table's columns and their types build by ColumnTypeBuilder
     * @param primaryKeyColumnNames     unique primary key column names
     */
    public MysqlDimensionTable5(String jdbcUrl,
                                String userName,
                                String password,
                                Duration refreshInterval,
                                String sql,
                                Map<String, Type> columnTypeMap,
                                String... primaryKeyColumnNames) {
        super(jdbcUrl, userName, password, refreshInterval, sql, columnTypeMap, primaryKeyColumnNames);
    }

    protected DataSource newDataSource() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(url);
        dataSource.setUser(userName);
        dataSource.setPassword(password);
        return dataSource;
    }
}
