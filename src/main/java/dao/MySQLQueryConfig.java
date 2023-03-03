package dao;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLQueryConfig {
    public static void main(String[] args) {

        final String url = "jdbc:mysql://dba:3306/battery?characterEncoding=utf8&useSSL=false";
        final String user = "battery";
        final String password = "Abcd.123";
        String query = "SELECT standard_field, equipment, custom_field FROM yanzheng_config WHERE type='自定义'";
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            Map<String, Map<String, String>> resultMap = new HashMap<>();
            while (resultSet.next()) {
                String standardField = resultSet.getString("standard_field");
                String equipment = resultSet.getString("equipment");
                String customField = resultSet.getString("custom_field");

                String[] equipmentArray = equipment.split("_");
                String groupKey = equipmentArray[0];

                if (!resultMap.containsKey(groupKey)) {
                    resultMap.put(groupKey, new HashMap<>());
                }
                Map<String, String> groupMap = resultMap.get(groupKey);
                groupMap.put(customField, standardField);
            }

            for (String groupKey : resultMap.keySet()) {
                Map<String, String> groupMap = resultMap.get(groupKey);
                System.out.println("Group key: " + groupKey);
                System.out.println("Group map: " + groupMap);
            }

            resultSet.close();
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}