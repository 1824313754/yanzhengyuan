package com;

import base.AbstractFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class MyFtpFileProcessor extends AbstractFtpFileProcessor<List<JSONObject>> {
    public static final String SERVER = "10.2.128.27";
    public static final int PORT = 2121;
    public static final String USERNAME = "2021112506";
    public static final String PASSWORD = "ftp@2021112506";

    public static final String ENCODING = "utf-8";
    public static final String REMOTE_FILE_PATH = "/数据分析平台数据库/BH 电性能数据/PACK测试/容量和能量/DJ2136/BHEPV-20220609016/PK202209009/-20℃";
    private static List<JSONObject> result= new ArrayList<>();

    @Override
    protected void doProcess(List<JSONObject> data) throws Exception {
        // 其他逻辑处理
        this.result.addAll(data);
    }

    public List<JSONObject> getResult() {
        return this.result;
    }
}
