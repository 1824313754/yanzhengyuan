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
    private static final String TABLE = "verification_dev.battery_data";
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
    public static void insertBatch(List<?> dataList) throws SQLException, IllegalAccessException {
        // 获取数据库连接
        Connection conn = ClickHouseUtils.getConnection();

        // 准备 SQL 语句和参数
        String sql = "INSERT INTO " + ClickHouseUtils.getTableName() + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // 创建 PreparedStatement 对象
        PreparedStatement pstmt = conn.prepareStatement(sql);

        // 使用反射设置 SQL 语句中的参数
        for (Object data : dataList) {
            Field[] fields = data.getClass().getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);
                Object value = field.get(data);
                pstmt.setString(i + 1, value == null ? null : value.toString());
            }
            pstmt.addBatch();
        }

        // 执行批量插入
        pstmt.executeBatch();

        // 关闭连接和资源
        pstmt.close();
        conn.close();
    }
}
