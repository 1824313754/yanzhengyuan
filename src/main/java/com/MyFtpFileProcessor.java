package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<Iterator<JSONObject>> {
    private List<JSONObject> result = new ArrayList<>();
    public MyFtpFileProcessor(Properties config) throws SQLException {
        super(config);
    }
    @Override
    protected void doProcess(Iterator<JSONObject> data) throws Exception {
        while (data.hasNext()) {
            JSONObject jsonObject = data.next();
            System.out.println(jsonObject.toJSONString());
            // 处理逻辑
            result.add(jsonObject);
        }
        //结果持久化
        saveBatch();
    }



    private void saveBatch() throws Exception {
        // 将当前文件的结果存储到文件系统上
        System.out.println("Saving batch: " + result.size());

        result.clear();
    }
}



