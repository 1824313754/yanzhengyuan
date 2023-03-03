package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<List<JSONObject>>  {
    private List<JSONObject> result = new ArrayList<>();
    //存放
    public MyFtpFileProcessor(Properties config) throws SQLException {
        super(config);
        getStandardConfigData();
    }
    Set<String> configDataKey = new HashSet<>();
    //所有公共的配置信息 (设备->(设备字段->标准化字段)))
    Map<String, Map<String, String>> configData = new HashMap<>();

    @Override
    protected void doProcess(List<JSONObject> data, String path) throws Exception {
        //找出configData的value对应的map的key的值是不是全都在data第一条数据的key中，如果是，就返回configData对应的key
        Set<String> execlHead = data.get(0).keySet();
        //遍历configData
        for (Map.Entry<String, Map<String, String>> entry : configData.entrySet()) {
            //定义一个变量，用来存放configData的value的key
            List<String> configDataValueKey = new ArrayList<>();
            //遍历configData的value
            for (Map.Entry<String, String> entry1 : entry.getValue().entrySet()) {
                //获取entry1的所有的key
                configDataValueKey.add(entry1.getKey());
            }
            //判断configDataValueKey是否包含execlHead
            if (execlHead.containsAll(configDataValueKey)) {
                configDataKey.add(entry.getKey());
            }
        }
        if(configDataKey!=null){
            //获取第一个值
            String first = configDataKey.stream().findFirst().get();
            //获取自定义的配置信息
            Map<String, String> customConfig = getStandardConfigData(configDataKey);
            //获取第一个值对应的map
            Map<String, String> firstMap = configData.get(first);
            for (JSONObject jsonObject : data) {
                JSONObject standardJson = new JSONObject();
                //定义2个数组温度和电压
                List<String> temperature = new ArrayList<>();
                List<String> voltage = new ArrayList<>();
                //公共字段映射
                for (Map.Entry<String, String> entry : firstMap.entrySet()) {
                    //获取标准化字段公共部分
                    String standardField = entry.getValue();
                    //获取设备字段
                    String equipmentField = entry.getKey();
                    //获取设备字段对应的值
                    String equipmentFieldValue = jsonObject.getString(equipmentField);
                    //将标准化字段和设备字段对应的值放入标准化json中
                    standardJson.put(standardField, equipmentFieldValue);
                }
                //遍历自定义的配置信息，将自定义的配置信息放入标准化json中
                for (Map.Entry<String, String> entry : customConfig.entrySet()) {
                    //获取标准化字段
                    String standardField = entry.getValue();
                    //获取设备字段
                    String equipmentField = entry.getKey();
                    //获取设备字段对应的值
                    Object equipmentFieldValue = jsonObject.get(equipmentField);
                    //将标准化字段和设备字段对应的值放入标准化json中,因为会有多个standardField重复可能会有空值覆盖之前的结果，只需要添加一次有值的即可
                    if(equipmentFieldValue!=null) {
                        standardJson.put(standardField, equipmentFieldValue);
                    }
                }
                if (!standardJson.containsKey("cellvoltMAX")) {
                    //获取jsonObject中包含不区分大小写的v，且包含不区分大小写的max或high,且不包含不区分大小写的T的key,且不包含不区分大小写的no的key
                    List<String> voltageMax = jsonObject.keySet().stream().filter(s -> s.toLowerCase().contains("vo") && (s.toLowerCase().contains("max") || s.toLowerCase().contains("high"))&&!s.toLowerCase().contains("no")).collect(Collectors.toList());
                    //若voltageMax的值为1，说明只有一个最大电压，直接获取最大电压的值
                    if (voltageMax.size() == 1) {
                        standardJson.put("cellvoltMAX", jsonObject.get(voltageMax.get(0)));
                    }
                }
                if (!standardJson.containsKey("cellvoltMIN")) {
                    //获取jsonObject中包含不区分大小写的v，且包含不区分大小写的min,且不包含不区分大小写的T的key
                    List<String> voltageMin = jsonObject.keySet().stream().filter(s -> s.toLowerCase().contains("vo") && (s.toLowerCase().contains("min") || s.toLowerCase().contains("low"))&&!s.toLowerCase().contains("no")).collect(Collectors.toList());
                    //若voltageMin的值为1，说明只有一个最小电压，直接获取最小电压的值
                    if (voltageMin.size() == 1) {
                        standardJson.put("cellvoltMIN", jsonObject.get(voltageMin.get(0)));
                    }
                }
                //温度
                if (!standardJson.containsKey("TemperatureMAX")) {
                    //获取jsonObject中包含不区分大小写的t，且包含不区分大小写的max或high,且不包含不区分大小写的V的key
                    List<String> temperatureMax = jsonObject.keySet().stream().filter(s -> s.toLowerCase().contains("tem") && (s.toLowerCase().contains("max") || s.toLowerCase().contains("high"))&&!s.toLowerCase().contains("no")).collect(Collectors.toList());
                    //若temperatureMax的值为1，说明只有一个最大温度，直接获取最大温度的值
                    if (temperatureMax.size() == 1) {
                        standardJson.put("TemperatureMAX", jsonObject.get(temperatureMax.get(0)));
                    }
                }
                if (!standardJson.containsKey("TemperatureMIN")) {
                    //获取jsonObject中包含不区分大小写的t，且包含不区分大小写的min或low,且不包含不区分大小写的V的key
                    List<String> temperatureMin = jsonObject.keySet().stream().filter(s -> s.toLowerCase().contains("tem") && (s.toLowerCase().contains("min") || s.toLowerCase().contains("low"))&&!s.toLowerCase().contains("no") &&!s.toLowerCase().contains("slow")).collect(Collectors.toList());
                    //若temperatureMin的值为1，说明只有一个最小温度，直接获取最小温度的值
                    if (temperatureMin.size() == 1) {
                        standardJson.put("TemperatureMIN", jsonObject.get(temperatureMin.get(0)));
                    }
                }
                //判断standardJson中是否有cellvolt加数字的key，如果有则将其按照数字放到指定的数组的下标中
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








                System.out.println(standardJson);
            }
        }
        configDataKey.clear();

    }


    private void saveBatch(Map<String, List<String>> configData) throws Exception {

        result.clear();
    }

    //查询标准化的配置信息公共字段
    public void getStandardConfigData() throws SQLException {
        String query = "SELECT standard_field, equipment, custom_field FROM yanzheng_config WHERE type='公共'";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            String standardField = resultSet.getString("standard_field");
            String equipment = resultSet.getString("equipment");
            String customField = resultSet.getString("custom_field");
            if (!configData.containsKey(equipment)) {
                configData.put(equipment, new HashMap<>());
            }
            Map<String, String> groupMap = configData.get(equipment);
            groupMap.put(customField, standardField);
        }

    }

    //查询标准化的配置信息自定义字段
    public Map<String, String> getStandardConfigData(Set<String> equipmentNames) throws SQLException {
        //自定义的配置信息 (设备->(设备字段->标准化字段)))
         Map<String, String> resultMap = new HashMap<>();
        String query = "SELECT standard_field, custom_field FROM yanzheng_config WHERE type='自定义' and equipment IN (" +
                equipmentNames.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + ")";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            String standardField = resultSet.getString("standard_field");
            String customField = resultSet.getString("custom_field");
            resultMap.put(customField, standardField);
        }
        return resultMap;
    }




}



