package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<List<JSONObject>> {
    private final List<JSONObject> result = new ArrayList<>();
    private final Map<String, Map<String, String>> configData = new HashMap<>();

    public MyFtpFileProcessor(Properties config) throws SQLException {
        super(config);
        getStandardConfigData();
    }

    @Override
    protected void doProcess(List<JSONObject> data, String path) throws Exception {
        Set<String> excelHeaders = data.get(0).keySet();
        Set<String> matchingConfigKeys = findMatchingConfigKeys(excelHeaders);

        if (matchingConfigKeys.isEmpty()) {
            //没有一个设备匹配
            return;
        }

        Map<String, String> customConfig = getCustomConfigData(matchingConfigKeys);

        for (JSONObject jsonObject : data) {
            JSONObject standardizedJson = standardizeJsonObject(jsonObject, matchingConfigKeys, customConfig);
            result.add(standardizedJson);
        }
        saveBatch();
    }
    //公共字段配置信息
    private Set<String> findMatchingConfigKeys(Set<String> excelHeaders) {
        Set<String> matchingConfigKeys = new HashSet<>();
        for (Map.Entry<String, Map<String, String>> entry : configData.entrySet()) {
            Set<String> configValueKeys = entry.getValue().keySet();
            if (excelHeaders.containsAll(configValueKeys)) {
                matchingConfigKeys.add(entry.getKey());
            }
        }
        return matchingConfigKeys;
    }
    //自定义字段配置信息
    private Map<String, String> getCustomConfigData(Set<String> matchingConfigKeys) throws SQLException {
        Map<String, String> customConfig = new HashMap<>();
        String query = "SELECT standard_field, custom_field FROM yanzheng_config WHERE type='自定义' and equipment IN (" +
                matchingConfigKeys.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + ")";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String standardField = resultSet.getString("standard_field");
                String customField = resultSet.getString("custom_field");
                customConfig.put(customField, standardField);
            }
        }
        return customConfig;
    }

    private JSONObject standardizeJsonObject(JSONObject jsonObject, Set<String> matchingConfigKeys, Map<String, String> customConfig) {
        JSONObject standardizedJson = new JSONObject();
        List<String> temperature = new ArrayList<>();
        List<String> voltage = new ArrayList<>();

        Map<String, String> firstMap = configData.get(matchingConfigKeys.iterator().next());
        for (Map.Entry<String, String> entry : firstMap.entrySet()) {
            String standardField = entry.getValue();
            String equipmentField = entry.getKey();
            String equipmentFieldValue = jsonObject.getString(equipmentField);
            standardizedJson.put(standardField, equipmentFieldValue);
        }

        for (Map.Entry<String, String> entry : customConfig.entrySet()) {
            String standardField = entry.getValue();
            String equipmentField = entry.getKey();
            Object equipmentFieldValue = jsonObject.get(equipmentField);
            if (equipmentFieldValue != null) {
                standardizedJson.put(standardField, equipmentFieldValue);
            }
        }

        if (!standardizedJson.containsKey("cellvoltMAX")) {
            List<String> voltageMax = jsonObject.keySet().stream()
                    .filter(s -> s.toLowerCase().contains("vo")
                            && (s.toLowerCase().contains("max") || s.toLowerCase().contains("high"))
                            && !s.toLowerCase().contains("no"))
                    .collect(Collectors.toList());
            if (voltageMax.size() == 1) {
                standardizedJson.put("cellvoltMAX", jsonObject.get(voltageMax.get(0)));
            }
        }
            if (!standardizedJson.containsKey("cellvoltMIN")) {
                List<String> voltageMin = jsonObject.keySet().stream()
                        .filter(s -> s.toLowerCase().contains("vo")
                                && (s.toLowerCase().contains("min") || s.toLowerCase().contains("low"))
                                && !s.toLowerCase().contains("no"))
                        .collect(Collectors.toList());
                if (voltageMin.size() == 1) {
                    standardizedJson.put("cellvoltMIN", jsonObject.get(voltageMin.get(0)));
                }
            }

            if (!standardizedJson.containsKey("TemperatureMAX")) {
                List<String> temperatureMax = jsonObject.keySet().stream()
                        .filter(s -> s.toLowerCase().contains("tem")
                                && (s.toLowerCase().contains("max") || s.toLowerCase().contains("high"))
                                && !s.toLowerCase().contains("no"))
                        .collect(Collectors.toList());
                if (temperatureMax.size() == 1) {
                    standardizedJson.put("TemperatureMAX", jsonObject.get(temperatureMax.get(0)));
                }
            }

            if (!standardizedJson.containsKey("TemperatureMIN")) {
                List<String> temperatureMin = jsonObject.keySet().stream()
                        .filter(s -> s.toLowerCase().contains("tem")
                                && (s.toLowerCase().contains("min") || s.toLowerCase().contains("low"))
                                && !s.toLowerCase().contains("no")
                                && !s.toLowerCase().contains("slow"))
                        .collect(Collectors.toList());
                if (temperatureMin.size() == 1) {
                    standardizedJson.put("TemperatureMIN", jsonObject.get(temperatureMin.get(0)));
                }
            }

            for (Map.Entry<String, Object> entry : standardizedJson.entrySet()) {
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
                if (key.toLowerCase().contains("temperature")) {
                    String substring = key.substring(11);
                    if (StringUtils.isNumeric(substring)) {
                        int index = Integer.parseInt(substring);
                        if (temperature.size() < index + 1) {
                            for (int i = temperature.size(); i < index + 1; i++) {
                                temperature.add(null);
                            }
                        }
                        temperature.set(index, value.toString());
                    }
                }
            }

            return standardizedJson;
        }


        private void getStandardConfigData () throws SQLException {
            String query = "SELECT standard_field, equipment, custom_field FROM yanzheng_config WHERE type='公共'";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {
                while (resultSet.next()) {
                    String standardField = resultSet.getString("standard_field");
                    String equipment = resultSet.getString("equipment");
                    String customField = resultSet.getString("custom_field");
                    configData.computeIfAbsent(equipment, k -> new HashMap<>()).put(customField, standardField);
                }
            }
        }

        private void saveBatch () throws Exception {
            result.forEach(System.out::println);
            result.clear();
        }
    }


