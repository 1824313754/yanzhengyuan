package bean;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StandardizedField {
    private String path;
    private String fileName;
    private String State;
    private String Voltage;
    private String Current;
    private String StepTime;
    private String TotTime;
    private String StepNo;
    private String Power;
    private String SOC;
    private String Type;
    private String CurCycle;
    private String TotCycle;
    private String CapaSum;
    private String CharCapAh;
    private String DischarCapAh;
    private String ChargeWh;
    private String DischargeWh;
    private String TotalEnergyWh;
    private String StartTime;
    private String EndTime;
    private String WorkingDate;
    private String WorkingTime;
    private String CellvoltMAX;
    private String CellvoltMIN;
    private String TemperatureMAX;
    private String TemperatureMIN;
    private String BatAvgCelVol;
    private String BatAvgTemp;
    private String DiffVol;
    //定义一个温度数组
    private String temperature;
    //定义一个电压数组
    private String cellvolt;

    private String processTime;

    //定义一个方法，将json转换为对象
    public static StandardizedField jsonToBean(JSONObject json) {
        JSONObject jsonObject = JSONObject.parseObject(json.toJSONString());
        //遍历修改json的key，转为该类的属性
        for (String key : jsonObject.keySet()) {
            if (key.equals("Voltage(V)")) {
                json.put("Voltage", json.get(key));
                json.remove(key);
            }
            if (key.equals("Current(A)")) {
                json.put("Current", json.get(key));
                json.remove(key);
            }
            if (key.equals("Power(W)")) {
                json.put("Power", json.get(key));
                json.remove(key);
            }
            if (key.equals("Capa. Sum")) {
                json.put("CapaSum", json.get(key));
                json.remove(key);
            }
            if (key.equals("Char. Cap.(Ah)")) {
                json.put("CharCapAh", json.get(key));
                json.remove(key);
            }
            if (key.equals("Dischar. Cap.(Ah)")) {
                json.put("DischarCapAh", json.get(key));
                json.remove(key);
            }
            if (key.equals("Start Time")) {
                json.put("StartTime", json.get(key));
                json.remove(key);
            }
            if (key.equals("End Time")) {
                json.put("EndTime", json.get(key));
                json.remove(key);
            }
            if (key.equals("Working Date")) {
                json.put("WorkingDate", json.get(key));
                json.remove(key);
            }
            if (key.equals("Working Time")) {
                json.put("WorkingTime", json.get(key));
                json.remove(key);
            }
            if (key.equals("ChargeWh(Wh)")) {
                json.put("ChargeWh", json.get(key));
                json.remove(key);
            }
            if (key.equals("DisChargeWh(Wh)")) {
                json.put("DischargeWh", json.get(key));
                json.remove(key);
            }
            if (key.equals("Total_energy(Wh)")) {
                json.put("TotalEnergyWh", json.get(key));
                json.remove(key);
            }
        }
        //将json转换为对象
        StandardizedField standardizedField = json.toJavaObject(StandardizedField.class);
        return standardizedField;
    }

    //重写toString方法
    @Override
    public String toString() {
        return "StandardizedField{" +
                "State='" + State + '\'' +
                ", Voltage='" + Voltage + '\'' +
                ", path='" + path + '\'' +
                ", Current='" + Current + '\'' +
                ", StepTime='" + StepTime + '\'' +
                ", TotTime='" + TotTime + '\'' +
                ", StepNo='" + StepNo + '\'' +
                ", Power='" + Power + '\'' +
                ", SOC='" + SOC + '\'' +
                ", Type='" + Type + '\'' +
                ", CurCycle='" + CurCycle + '\'' +
                ", TotCycle='" + TotCycle + '\'' +
                ", CapaSum='" + CapaSum + '\'' +
                ", CharCapAh='" + CharCapAh + '\'' +
                ", DischarCapAh='" + DischarCapAh + '\'' +
                ", ChargeWh='" + ChargeWh + '\'' +
                ", DischargeWh='" + DischargeWh + '\'' +
                ", TotalEnergyWh='" + TotalEnergyWh + '\'' +
                ", StartTime='" + StartTime + '\'' +
                ", EndTime='" + EndTime + '\'' +
                ", WorkingDate='" + WorkingDate + '\'' +
                ", WorkingTime='" + WorkingTime + '\'' +
                ", CellvoltMAX='" + CellvoltMAX + '\'' +
                ", CellvoltMIN='" + CellvoltMIN + '\'' +
                ", TemperatureMAX='" + TemperatureMAX + '\'' +
                ", TemperatureMIN='" + TemperatureMIN + '\'' +
                ", BatAvgCelVol='" + BatAvgCelVol + '\'' +
                ", BatAvgTemp='" + BatAvgTemp + '\'' +
                ", DiffVol='" + DiffVol + '\'' +
                ", temperature=" + temperature +
                ", voltageArray=" + cellvolt +
                '}';
    }
}

