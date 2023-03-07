package utils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class ClickHouseUtils {

    // 数据库连接信息
    private static final String USER = "default";
    private static final String PASSWORD = "battery@123";
    private static final String TABLE = "verification_dev.standard";
    private static final String CONN = "jdbc:clickhouse://test01:8123";

    // 加载 ClickHouse JDBC 驱动
    static {
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 获取数据库连接
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(CONN, USER, PASSWORD);
    }

    // 获取表名
    public static String getTableName() {
        return TABLE;
    }

}
