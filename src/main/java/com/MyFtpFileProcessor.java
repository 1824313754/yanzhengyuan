package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;
import java.util.*;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<List<JSONObject>> {
    private List<JSONObject> result = new ArrayList<>();
    //存放
    public MyFtpFileProcessor(Properties config) throws SQLException {
        super(config);
    }
    List<String> configDataKey = new ArrayList<>();
    @Override
    protected void doProcess(List<JSONObject> data, String path, Map<String, Map<String, String>> configData) throws Exception {
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
                System.out.println(path+" "+ entry.getKey());
            }
        }





//        configData.forEach((key, value) -> {
//            if (execlHead.containsAll(value)) {
//                System.out.println(path+" "+ key);
//            }
//        });
    }


    private void saveBatch(Map<String, List<String>> configData) throws Exception {
        //打印configData
//        for (String[] strings : configData) {
//            System.out.println(Arrays.toString(strings));
//        }
//        result.forEach(System.out::println);
//        System.out.println("Saving batch: " + result.size());
        //将结果存入
        result.clear();
    }
}



