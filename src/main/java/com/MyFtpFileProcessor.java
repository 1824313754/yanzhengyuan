package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;
import java.util.*;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<Iterator<JSONObject>> {
    private List<JSONObject> result = new ArrayList<>();
    public MyFtpFileProcessor(Properties config) throws SQLException {
        super(config);
    }
    @Override
    protected void doProcess(Iterator<JSONObject> data) throws Exception {
        while (data.hasNext()) {
            JSONObject jsonObject = data.next();
//            System.out.println(jsonObject.toJSONString());
            // 处理逻辑
            result.add(jsonObject);
        }
        //结果持久化
        saveBatch();
    }



    private void saveBatch() throws Exception {
//        result.forEach(System.out::println);
        System.out.println("Saving batch: " + result.size());
        //将结果存入
        result.clear();
    }
}



