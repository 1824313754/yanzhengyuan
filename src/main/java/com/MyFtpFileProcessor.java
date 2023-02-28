package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<List<JSONObject>> {
    public static final String SERVER = "10.2.128.27";
    public static final int PORT = 2121;
    public static final String USERNAME = "2021112506";
    public static final String PASSWORD = "ftp@2021112506";
    public static final String REMOTE_FILE_PATH = "/数据分析平台数据库/BH 电性能数据/PACK测试/容量和能量/DJ2136/BHEPV-20220609016/PK202209009/-20℃";
    private List<JSONObject> result = new ArrayList<>();

    public MyFtpFileProcessor(Properties config) throws SQLException {
        super(config);
    }

    @Override
    protected void doProcess(List<JSONObject> data) throws Exception {
        // Other logic to process data
        result.addAll(data);
    }

    public List<JSONObject> getResult() {
        return result;
    }
}

