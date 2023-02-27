package dao;

import base.DatabaseService;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class SimpleDatabaseService implements DatabaseService {
    private static final String URL = "jdbc:mysql://localhost:3306/mydb?serverTimezone=UTC&useSSL=false";
    private static final String USER = "username";
    private static final String PASSWORD = "password";

    @Override
    public void logProcessedFile(String filePath, boolean success, LocalDateTime processTime) throws Exception {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            String sql = "INSERT INTO pathprocess (path, success, process_time) VALUES (?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, filePath);
            statement.setBoolean(2, success);
            statement.setTimestamp(3, Timestamp.valueOf(processTime));
            statement.executeUpdate();
        }
    }

    @Override
    public List<String> getProcessedFiles() throws Exception {
        List<String> result = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            String sql = "SELECT path FROM pathprocess WHERE success = ?";
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setBoolean(1, true);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                result.add(resultSet.getString("path"));
            }
        }
        return result;
    }
}

