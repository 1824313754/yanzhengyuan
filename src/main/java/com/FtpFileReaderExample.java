package com;

import com.alibaba.fastjson.JSONObject;

import java.util.List;

public class FtpFileReaderExample {
    public static void main(String[] args) {
        MyFtpFileProcessor processor = new MyFtpFileProcessor();
        try {
            processor.process(MyFtpFileProcessor.SERVER, MyFtpFileProcessor.PORT, MyFtpFileProcessor.USERNAME, MyFtpFileProcessor.PASSWORD, MyFtpFileProcessor.REMOTE_FILE_PATH, "csv");
            List<JSONObject> result = processor.getResult();
            for (JSONObject item : result) {
                System.out.println(item.toJSONString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

