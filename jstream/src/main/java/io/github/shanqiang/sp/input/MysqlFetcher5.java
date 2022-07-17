package io.github.shanqiang.sp.input;

import com.shaded.mysql5.jdbc.jdbc2.optional.MysqlDataSource;
import io.github.shanqiang.table.Type;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class MysqlFetcher5 extends JdbcFetcher {
    public MysqlFetcher5(String jdbcUrl,
                         String userName,
                         String password,
                         Map<String, Type> columnTypeMap) throws SQLException {
        super(columnTypeMap, connection(jdbcUrl, userName, password));
    }

    private static Connection connection(String jdbcUrl,
                                         String userName,
                                         String password) throws SQLException {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(jdbcUrl);
        dataSource.setUser(userName);
        dataSource.setPassword(password);
        dataSource.setAutoReconnect(true);
        return dataSource.getConnection();
    }
}
