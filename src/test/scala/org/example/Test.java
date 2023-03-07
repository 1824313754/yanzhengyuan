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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] args) {
//        //定义一个数组
//        List<String> voltage = new ArrayList<>();
//        JSONObject standardJson = new JSONObject();
//        standardJson.put("cellVolt1", "1");
//        standardJson.put("cellVolt3", "3");
//        for (Map.Entry<String, Object> entry : standardJson.entrySet()) {
//            String key = entry.getKey();
//            Object value = entry.getValue();
//            if (key.toLowerCase().contains("cellvolt")) {
//                String substring = key.substring(8);
//                if (StringUtils.isNumeric(substring)) {
//                    int index = Integer.parseInt(substring);
//                    if (voltage.size() < index + 1) {
//                        for (int i = voltage.size(); i < index + 1; i++) {
//                            voltage.add(null);
//                        }
//                    }
//                    voltage.set(index, value.toString());
//                }
//            }
//        }
//        System.out.println(voltage);

            JSONObject jsonObject = new JSONObject();
        jsonObject.put("Aux. max Volt","1");
        jsonObject.put("Au max Volt","1");

        String shortestKeyWithHighOrMaxContainingVoltOrCell = matchJsonObjectKey(jsonObject);
        System.out.println(shortestKeyWithHighOrMaxContainingVoltOrCell);
    }
    public static String matchJsonObjectKey(JSONObject jsonObj) {
        String patternStr = ".*?(high|max).*?(volt|cell).*|.*?(volt|cell).*?(high|max).*";
        Pattern pattern = Pattern.compile(patternStr);
        String shortestKey = null;
        int shortestLength = Integer.MAX_VALUE;

        for (String key : jsonObj.keySet()) {
            Matcher matcher = pattern.matcher(key.toLowerCase());
            if (matcher.find()) {
                String matchKey = matcher.group();
                if (matchKey.length() < shortestLength) {
                    shortestKey = key;
                    shortestLength = matchKey.length();
                }
            }
        }
        return shortestKey;
    }

//        public static void main(String[] args) throws SQLException, ClassNotFoundException {
//            Class<?> generatedClass = ClickHouseTableToJava.generateClassFromClickHouse("MyClass");
//            System.out.println("Generated class: " + generatedClass.getName());
//        }
    }


