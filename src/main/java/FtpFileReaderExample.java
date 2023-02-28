import com.MyFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

import java.util.List;

public class FtpFileReaderExample {
    public static void main(String[] args) throws Exception {
        String mysqlUrl = "jdbc:mysql://dba:3306/battery?characterEncoding=utf8&useSSL=false";
        String mysqlUser = "battery";
        String mysqlPassword = "Abcd.123";
        MyFtpFileProcessor fileProcessor = new MyFtpFileProcessor(mysqlUrl, mysqlUser, mysqlPassword);
        fileProcessor.process(MyFtpFileProcessor.SERVER, MyFtpFileProcessor.PORT, MyFtpFileProcessor.USERNAME, MyFtpFileProcessor.PASSWORD, MyFtpFileProcessor.REMOTE_FILE_PATH, "csv");
        List<JSONObject> processedData = fileProcessor.getResult();
        //打印处理后的数据
        processedData.forEach(System.out::println);
    }

}

