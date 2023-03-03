package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

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



