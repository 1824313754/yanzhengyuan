package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<List<JSONObject>> {
    // 定义一个List类型的集合result，用于保存标准化后的JSONObject对象
    private final List<JSONObject> result = new ArrayList<>();
    // 定义一个Map类型的集合configData，用于保存公共字段配置信息
    private final Map<String, Map<String, String>> configData = new HashMap<>();

    // 构造函数，接收一个Properties类型的参数config，并抛出SQLException异常
    public MyFtpFileProcessor(Properties config) throws SQLException {
        super(config);
        // 获取公共字段配置信息
        getStandardConfigData();
    }

    // doProcess方法，接收两个参数：一个List类型的数据data和一个String类型的路径path。该方法还抛出Exception异常。
    @Override
    protected void doProcess(List<JSONObject> data, String path) throws Exception {
        // 从data中获取到一个JSONObject对象的Set集合excelHeaders
        Set<String> excelHeaders = data.get(0).keySet();
        // 使用findMatchingConfigKeys方法找到与excelHeaders匹配的配置数据的key，并将这些key保存在一个Set集合matchingConfigKeys中
        Set<String> matchingConfigKeys = findMatchingConfigKeys(excelHeaders);

        // 如果matchingConfigKeys为空，则表示没有设备匹配，直接返回
        if (matchingConfigKeys.isEmpty()) {
            return;
        }

        // 使用getCustomConfigData方法获取与matchingConfigKeys匹配的自定义配置数据，并将这些数据保存在一个Map类型的集合customConfig中
        Map<String, String> customConfig = getCustomConfigData(matchingConfigKeys);

        // 对于data中的每个JSONObject对象，都将其标准化为一个新的JSONObject对象standardizedJson，并将其添加到result集合中
        for (JSONObject jsonObject : data) {
//            System.out.println("正在处理的数据：" + jsonObject);
            JSONObject standardizedJson = standardizeJsonObject(jsonObject, matchingConfigKeys, customConfig,path);
            result.add(standardizedJson);
        }

        // 将result集合中的数据保存到数据库中
        saveBatch();
    }

    // saveBatch方法，将result集合中的数据打印出来并清空result集合
    private void saveBatch() {
        result.forEach(System.out::println);
        result.clear();
    }

    // findMatchingConfigKeys方法，找到与excelHeaders匹配的配置数据的key，并将这些key保存在一个Set集合matchingConfigKeys中
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

    // standardizeJsonObject方法，将一个JSONObject对象标准化为一个新的JSONObject对象
    private JSONObject standardizeJsonObject(JSONObject jsonObject, Set<String> matchingConfigKeys, Map<String, String> customConfig,String path) {
        // 创建一个新的JSONObject对象standardizedJson
        JSONObject standardizedJson = new JSONObject();
        //文件绝对路径添加
        standardizedJson.put("path",path);
        // 将data中JSONObject对象的公共字段添加到standardizedJson中
        Map<String, String> firstMap = configData.get(matchingConfigKeys.iterator().next());
        for (Map.Entry<String, String> entry : firstMap.entrySet()) {
            String standardField = entry.getValue();
            String equipmentField = entry.getKey();
            String equipmentFieldValue = jsonObject.getString(equipmentField);
            standardizedJson.put(standardField, equipmentFieldValue);
        }

        // 使用自定义配置数据customConfig来将data中的自定义字段添加到standardizedJson中
        for (Map.Entry<String, String> entry : customConfig.entrySet()) {
            String standardField = entry.getValue();
            String equipmentField = entry.getKey();
            Object equipmentFieldValue = jsonObject.get(equipmentField);
            if (equipmentFieldValue != null) {
                standardizedJson.put(standardField, equipmentFieldValue);
            }
        }

        // 使用addStandardizedJsonByKeys方法将data中的某些字段添加到standardizedJson中
        Map<String, List<List<String>>> keysAndStrings = new HashMap<>();
        keysAndStrings.put("cellvoltMAX", Arrays.asList(Arrays.asList("vo", "cel"), Arrays.asList("no"), Arrays.asList("max", "high")));
        keysAndStrings.put("cellvoltMIN", Arrays.asList(Arrays.asList("vo", "cel"), Arrays.asList("no", "slow"), Arrays.asList("min", "low")));
        keysAndStrings.put("TemperatureMAX", Arrays.asList(Arrays.asList("tem"), Arrays.asList("no"), Arrays.asList("max", "high")));
        keysAndStrings.put("TemperatureMIN", Arrays.asList(Arrays.asList("tem"), Arrays.asList("no", "slow"), Arrays.asList("min", "low")));
        addStandardizedJsonByKeys(standardizedJson, jsonObject, keysAndStrings);
        addTemperatureByNumber(standardizedJson, jsonObject, "temperature","temp");
        addTemperatureByNumber(standardizedJson, jsonObject, "cellvolt","vol");
       //定义2个List集合，用于保存温度和电压数据
        List<String> temperature = new ArrayList<>();
        List<String> voltage = new ArrayList<>();
        // 将standardizedJson中的温度和电压数据保存到temperature和voltage集合中
        for (Map.Entry<String, Object> entry : standardizedJson.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            addValueToListAtIndex(key, value, temperature, "temperature");
            addValueToListAtIndex(key, value, voltage, "cellvolt");
            //将temperature和voltage集合中的数据转换为字符串，并添加到standardizedJson中
        }
        standardizedJson.put("temperature", temperature.toString());
        standardizedJson.put("cellvolt", voltage.toString());
        // 返回标准化后的JSONObject对象
        return standardizedJson;
    }
    public static void addValueToListAtIndex(String key, Object value, List<String> list, String prefix) {
        String regex = prefix + "(\\d+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(key);

        if (matcher.matches()) {
            int index = Integer.parseInt(matcher.group(1)) - 1;
            if (index >= 0) {
                if (list.size() < index + 1) {
                    for (int i = list.size(); i < index + 1; i++) {
                        list.add(null);
                    }
                }
                list.set(index, value.toString());
            }
        }
    }



    // getStandardConfigData方法，获取公共字段配置信息并保存在configData集合中
    private void getStandardConfigData() throws SQLException {
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
    // getCustomConfigData方法，获取与matchingConfigKeys匹配的自定义配置数据，并将这些数据保存在一个Map类型的集合customConfig中
    private Map<String, String> getCustomConfigData(Set<String> matchingConfigKeys) throws SQLException {
        Map<String, String> customConfig = new HashMap<>();
        String query = "SELECT standard_field, custom_field FROM yanzheng_config WHERE type='自定义' and equipment IN (" + matchingConfigKeys.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + ")";
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

    // addStandardizedJsonByKeys方法，将data中的某些字段添加到standardizedJson中
    public static void addStandardizedJsonByKeys(JSONObject standardizedJson, JSONObject jsonObject, Map<String, List<List<String>>> keysAndStrings) {
        for (Map.Entry<String, List<List<String>>> entry : keysAndStrings.entrySet()) {
            String key = entry.getKey();
            List<String> mustMatchStrings = entry.getValue().get(0);
            List<String> notMatchStrings = entry.getValue().get(1);
            List<String> containsStrings = entry.getValue().get(2);

            if (!standardizedJson.containsKey(key)) {
                List<String> filteredKeys = jsonObject.keySet().stream()
                        .filter(s -> mustMatchStrings.stream().anyMatch(s.toLowerCase()::contains))
                        .filter(s -> notMatchStrings.stream().noneMatch(s.toLowerCase()::contains))
                        .filter(s -> containsStrings.stream().anyMatch(s.toLowerCase()::contains))
                        .collect(Collectors.toList());

                if (filteredKeys.size() == 1) {
                    standardizedJson.put(key, jsonObject.get(filteredKeys.get(0)));
                }
            }
        }
    }


    public  void addTemperatureByNumber(JSONObject standardizedJson, JSONObject jsonObject, String temperatureKeyPrefix, String temperatureKeyAlt) {
        for (String key : standardizedJson.keySet()) {
            if (key.matches(temperatureKeyPrefix + "\\d+")) {
                return;
            }
        }
        Pattern pattern = Pattern.compile(".*CAN.*" + temperatureKeyAlt + "\\D*(\\d+).*");
        for (String key : jsonObject.keySet()) {
            Matcher matcher = pattern.matcher(key);
            if (matcher.matches()) {
                String number = matcher.group(1);
                String temperatureKey = temperatureKeyPrefix + number;
                if (!standardizedJson.containsKey(temperatureKey)) {
                    standardizedJson.put(temperatureKey, jsonObject.get(key));
                }
            }
        }
    }


}


