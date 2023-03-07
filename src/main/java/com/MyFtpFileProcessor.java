package com;

import base.AbstractFtpFileProcessor;
import bean.StandardizedField;
import com.alibaba.fastjson.JSONObject;
import utils.ClickHouseUtils;

import java.lang.reflect.Field;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static bean.StandardizedField.jsonToBean;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<List<JSONObject>> {
    // 定义一个Map类型的集合,用于存放正则表达式匹配出来的字段名和字段值
    private final Map<String, String> regexMap = new HashMap<>();
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
        // 定义一个List类型的集合result，用于保存标准化后的JSONObject对象
        List<JSONObject> result = new ArrayList<>();
        // 从data中获取到一个JSONObject对象的Set集合excelHeaders
        Set<String> excelHeaders = data.get(0).keySet();
        // 使用findMatchingConfigKeys方法找到与excelHeaders匹配的配置数据的key，并将这些key保存在一个Set集合matchingConfigKeys中
        Set<String> matchingConfigKeys = findMatchingConfigKeys(excelHeaders);
        matchJsonObjectKey(data.get(0), "cellvoltMAX", ".*?(high|max).*?(volt|cell).*|.*?(volt|cell).*?(high|max).*");
        matchJsonObjectKey(data.get(0),"cellvoltMIN", ".*?(low|min).*?(volt|cell).*|.*?(volt|cell).*?(low|min).*");
        matchJsonObjectKey(data.get(0),"temperatureMAX", ".*?(high|max).*?(temp).*|.*?(temp).*?(high|max).*");
        matchJsonObjectKey(data.get(0),"temperatureMIN", ".*?(low|min).*?(temp).*|.*?(temp).*?(low|min).*");


        // 如果matchingConfigKeys为空，则表示没有设备匹配，直接返回
        if (matchingConfigKeys.isEmpty()) {
            //抛异常，终止程序
            throw new Exception("没有匹配的设备");
        }

        // 使用getCustomConfigData方法获取与matchingConfigKeys匹配的自定义配置数据，并将这些数据保存在一个Map类型的集合customConfig中
        Map<String, String> customConfig = getCustomConfigData(matchingConfigKeys);
        //定义一个map存放自定义的字段映射
        Map<String, String> customMap = new HashMap<>();
        for (Map.Entry<String, String> entry : customConfig.entrySet()) {
            String standardField = entry.getValue();
            String equipmentField = entry.getKey();
            if(excelHeaders.contains(equipmentField)){
                customMap.put(equipmentField,standardField);
            }
        }

        // 对于data中的每个JSONObject对象，都将其标准化为一个新的JSONObject对象standardizedJson，并将其添加到result集合中
        for (JSONObject jsonObject : data) {
//            System.out.println("正在处理的数据：" + jsonObject);
            JSONObject standardizedJson = standardizeJsonObject(jsonObject, matchingConfigKeys, customMap,path);
            result.add(standardizedJson);
        }
        saveBatch(result);
        regexMap.clear();
    }

    // saveBatch方法，将result集合中的数据打印出来并清空result集合
    private  void saveBatch(List<JSONObject> result) throws SQLException, IllegalAccessException {
        Connection conn = ClickHouseUtils.getConnection();
        // 准备 SQL 语句和参数
        String sql = "INSERT INTO " + ClickHouseUtils.getTableName() + " VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // 创建 PreparedStatement 对象
        PreparedStatement pstmt = conn.prepareStatement(sql);
        //循环遍历result集合
        for (JSONObject jsonObject : result) {
            //打印result集合中的数据
            StandardizedField standardizedField = jsonToBean(jsonObject);
            Field[] fields = standardizedField.getClass().getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);
                Object value = field.get(standardizedField);
                pstmt.setString(i + 1, value == null ? null : value.toString());
            }
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        conn.close();
//        result.forEach(System.out::println);
    }

    /**
     * findMatchingConfigKeys方法，找到与excelHeaders匹配的配置数据的key，并将这些key保存在一个Set集合matchingConfigKeys中
     * 若没有全部匹配，则返回最接近的匹配，打印出缺少的字段
     * @param excelHeaders
     * @return
     */
    private Set<String> findMatchingConfigKeys(Set<String> excelHeaders) {
        Set<String> matchingConfigKeys = new HashSet<>();
        int maxMatchCount = 0;
        String maxMatchKey = null;
        Set<String> maxMatchKeyMissingElements = new HashSet<>();
        for (Map.Entry<String, Map<String, String>> entry : configData.entrySet()) {
            Set<String> configValueKeys = entry.getValue().keySet().stream().filter(key -> !key.equals("")).collect(Collectors.toSet());
            int matchCount = 0;
            Set<String> missingElements = new HashSet<>(configValueKeys);
            missingElements.removeAll(excelHeaders);
            for (String header : excelHeaders) {
                if (configValueKeys.contains(header)) {
                    matchCount++;
                    missingElements.remove(header);
                }
            }
            if (matchCount == configValueKeys.size()) {
                matchingConfigKeys.add(entry.getKey());
            } else if (matchCount > maxMatchCount) {
                maxMatchCount = matchCount;
                maxMatchKey = entry.getKey();
                maxMatchKeyMissingElements = missingElements;
            }
        }
        if (matchingConfigKeys.isEmpty() && maxMatchKey != null) {
            System.out.println("No exact match found, closest match is key " + maxMatchKey +
                    " with " + maxMatchCount + " matching elements and missing elements: " +
                    maxMatchKeyMissingElements);
        }
        return matchingConfigKeys;
    }


    // standardizeJsonObject方法，将一个JSONObject对象标准化为一个新的JSONObject对象
    private JSONObject standardizeJsonObject(JSONObject jsonObject, Set<String> matchingConfigKeys, Map<String, String> customMap,String path) {
        // 创建一个新的JSONObject对象standardizedJson
        JSONObject standardizedJson = new JSONObject();
        standardizedJson.put("path", path.substring(0, path.lastIndexOf("/")));
        //文件绝对路径添加
        standardizedJson.put("fileName",path.split("/")[path.split("/").length-1]);
        // 将data中JSONObject对象的公共字段添加到standardizedJson中
        Map<String, String> firstMap = configData.get(matchingConfigKeys.iterator().next());
        for (Map.Entry<String, String> entry : firstMap.entrySet()) {
            String standardField = entry.getValue();
            String equipmentField = entry.getKey();
            String equipmentFieldValue = jsonObject.getString(equipmentField);
            standardizedJson.put(standardField, equipmentFieldValue);
        }

        // 使用自定义配置数据customConfig来将data中的自定义字段添加到standardizedJson中
        for (Map.Entry<String, String> entry : customMap.entrySet()) {
            String standardField = entry.getValue();
            String equipmentField = entry.getKey();
            Object equipmentFieldValue = jsonObject.get(equipmentField);
            if (equipmentFieldValue != null) {
                standardizedJson.put(standardField, equipmentFieldValue);
            }
        }
        //若standardizedJson的key没有cellvoltMAX，cellvoltMIN，TemperatureMAX，TemperatureMIN则分别使用正则表达式寻找jsonObject的key
        if (!standardizedJson.containsKey("cellvoltMAX")) {
            standardizedJson.put("cellvoltMAX",jsonObject.getString(regexMap.get("cellvoltMAX")));
        }
        if (!standardizedJson.containsKey("cellvoltMIN")) {
            standardizedJson.put("cellvoltMIN",jsonObject.getString(regexMap.get("cellvoltMIN")));
        }
        if (!standardizedJson.containsKey("TemperatureMAX")) {
            standardizedJson.put("TemperatureMAX",jsonObject.getString(regexMap.get("TemperatureMAX")));
        }
        if (!standardizedJson.containsKey("TemperatureMIN")) {
            standardizedJson.put("TemperatureMIN",jsonObject.getString(regexMap.get("TemperatureMIN")));
        }
        // 使用addStandardizedJsonByKeys方法将data中的某些字段添加到standardizedJson中
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
        }
        standardizedJson.put("temperature", temperature.toString());
        standardizedJson.put("cellvolt", voltage.toString());
        //放入当前时间，格式为yyyy-MM-dd HH:mm:ss
        standardizedJson.put("processTime",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        // 返回标准化后的JSONObject对象
        return standardizedJson;
    }
    public  void addValueToListAtIndex(String key, Object value, List<String> list, String prefix) {
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

    //匹配
    public  String matchJsonObjectKey(JSONObject jsonObj,String keyName,String patternStr) {
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
        regexMap.put(keyName, patternStr);
        return shortestKey;
    }




}


