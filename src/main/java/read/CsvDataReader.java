package read;

import com.alibaba.fastjson.JSONObject;
import base.DataReader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CsvDataReader implements DataReader<Iterator<JSONObject>> {
    @Override
    public Iterator<JSONObject> read(InputStream inputStream) throws Exception {
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
                    String value = i < values.length ? values[i] : null; // 防止数组越界，缺失值用 null 表示
                    headers[i] = headers[i].replaceAll("\\(.*\\)", "");
                    jsonObject.put(headers[i], value);
                }
                resultList.add(jsonObject);
            }
        }
        reader.close();
        return resultList.iterator();
    }
}
