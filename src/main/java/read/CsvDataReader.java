package read;

import com.alibaba.fastjson.JSONObject;
import base.DataReader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CsvDataReader implements DataReader<List<JSONObject>> {
    @Override
    public List<JSONObject> read(InputStream inputStream) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "GBK"));
        String line;
        String[] headers = null;
        List<JSONObject> resultList = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            if (headers == null) {
                headers = line.split(",");
            } else {
                String[] values = line.split(",");
                JSONObject jsonObject = new JSONObject();
                for (int i = 0; i < headers.length; i++) {
                    String value = i < values.length ? values[i] : " "; // 防止数组越界，缺失值用空格表示
//                    headers[i] = headers[i].replaceAll("\\(.*\\)", "");
                    jsonObject.put(headers[i].trim(), value);
                }
                resultList.add(jsonObject);
            }
        }
        reader.close();
        return resultList;
    }
}
