import com.MyFtpFileProcessor;
import com.alibaba.fastjson.JSONObject;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

public class FtpFileReaderExample {
    public static void main(String[] args) throws Exception {
        Properties config = new Properties();
        InputStream inputStream = new FileInputStream("config.properties");
        Reader reader = new InputStreamReader(inputStream, Charset.forName("UTF-8"));
        config.load(reader);
        MyFtpFileProcessor processor = new MyFtpFileProcessor(config);
        processor.process(config, "csv");

    }

}

