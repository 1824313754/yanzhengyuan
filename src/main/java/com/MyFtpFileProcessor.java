package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<Iterator<JSONObject>> {
    private int batchSize = 1000; // 每批处理的文件数量
    private int batchCount = 0; // 当前批次处理的文件数量
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
            batchCount++;
            if (batchCount >= batchSize) {
                saveBatch(); // 每处理完一批文件就保存一次结果
                batchCount = 0;
            }
        }
    }

    private void saveBatch() throws Exception {
        // 可以将数据写入数据库、文件系统等
        System.out.println("Saving batch: " + result.size());
        result.clear();
    }
}


