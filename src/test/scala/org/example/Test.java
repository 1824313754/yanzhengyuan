package org.example;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Test {
    public static void main(String[] args) {
        //定义一个数组
        List<String> voltage = new ArrayList<>();
        JSONObject standardJson = new JSONObject();
        standardJson.put("cellVolt1", "1");
        standardJson.put("cellVolt3", "3");
        for (Map.Entry<String, Object> entry : standardJson.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.toLowerCase().contains("cellvolt")) {
                String substring = key.substring(8);
                if (StringUtils.isNumeric(substring)) {
                    int index = Integer.parseInt(substring);
                    if (voltage.size() < index + 1) {
                        for (int i = voltage.size(); i < index + 1; i++) {
                            voltage.add(null);
                        }
                    }
                    voltage.set(index, value.toString());
                }
            }
        }
        System.out.println(voltage);
    }

//        public static void main(String[] args) throws SQLException, ClassNotFoundException {
//            Class<?> generatedClass = ClickHouseTableToJava.generateClassFromClickHouse("MyClass");
//            System.out.println("Generated class: " + generatedClass.getName());
//        }
    }


