package read;

import com.alibaba.fastjson.JSONObject;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import base.DataReader;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class XlsDataReader implements DataReader<List<JSONObject>> {
    @Override
    public List<JSONObject> read(InputStream inputStream) throws Exception {
        List<JSONObject> resultList = new ArrayList<>();
        Workbook workbook = new HSSFWorkbook(inputStream);
        Sheet sheet = workbook.getSheetAt(0);
        Row headerRow = sheet.getRow(0);
        Iterator<Row> rowIterator = sheet.iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            if (row.getRowNum() == 0) {
                continue;
            }
            JSONObject jsonObject = new JSONObject();
            for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
                if (cell != null) {
                    switch (cell.getCellType()) {
                        case STRING:
                            jsonObject.put(headerRow.getCell(i).getStringCellValue(), cell.getStringCellValue());
                            break;
                        case NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)) {
                                jsonObject.put(headerRow.getCell(i).getStringCellValue(), cell.getDateCellValue());
                            } else {
                                jsonObject.put(headerRow.getCell(i).getStringCellValue(), cell.getNumericCellValue());
                            }
                            break;
                        case BOOLEAN:
                            jsonObject.put(headerRow.getCell(i).getStringCellValue(), cell.getBooleanCellValue());
                            break;
                        default:
                            jsonObject.put(headerRow.getCell(i).getStringCellValue(), "");
                    }
                }
            }
            resultList.add(jsonObject);
        }
        inputStream.close();
        return resultList;
    }
}
