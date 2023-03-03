package org.example;

import java.io.*;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class InsertData {

    public static void main(String[] args) {
        String inputFile = "F:\\yanzhengyuan\\data_analysis\\data\\a.txt";
        String tableName = "yanzheng_config";
        String column1Name = "standard_field";
        String column2Name = "equipment";
        String column3Name = "custom_field";
        String column4Name = "type";
        String column4Value = null;
        //定义一个数组  包含Voltage(V) Current(A) Char. Cap.(Ah) Dischar. Cap.(Ah) ChargeWh(Wh) DisChargeWh(Wh) cellvoltMAX cellvoltMIN TemperatureMAX TemperatureMIN
        List<String> valuesList = new ArrayList<>();
        valuesList.add("Voltage(V)");
        valuesList.add("Current(A)");
        valuesList.add("StepTime");
        valuesList.add("TotTime");
        valuesList.add("StepNo");
        valuesList.add("Power(W)");
        valuesList.add("Type");
        valuesList.add("CurCycle");
        valuesList.add("TotCycle");
        valuesList.add("Capa. Sum");
        valuesList.add("Char. Cap.(Ah)");
        valuesList.add("Dischar. Cap.(Ah)");
        valuesList.add("ChargeWh(Wh)");
        valuesList.add("DisChargeWh(Wh)");
        valuesList.add("Total_energy(Wh)");
        valuesList.add("Start Time");
        valuesList.add("End Time");
        valuesList.add("Working Date");
        valuesList.add("Working Time");
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            String[] firstRow = null;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\t");
                if (firstRow == null) {
                    firstRow = values;
                } else {
                    if (valuesList.contains(values[0].trim())) {
                        column4Value = "公共";
                    }else {
                        column4Value = "自定义";
                    }
                    for (int i = 1; i < values.length; i++) {
                        //若第一列的值在valuesList中，则column4Name的值为"公共"
                        String sql="INSERT INTO " + tableName + " (" + column1Name + ", " + column2Name + ", " + column3Name + ", " + column4Name + ") VALUES ('" + values[0].trim() + "', '" + firstRow[i] + "', '" + values[i].trim() + "', '" + column4Value + "');";
                        //通过io流写入文件b.txt
                        FileWriter fw = new FileWriter("F:\\yanzhengyuan\\data_analysis\\data\\b.txt",true);
                        fw.write(sql);
                        fw.write("\r");
                        fw.close();
//                        System.out.println("INSERT INTO " + tableName + " (" + column1Name + ", " + column2Name + ", " + column3Name + ") VALUES ('" + values[0] + "', '" + firstRow[i] + "', '" + values[i] + "');");
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}






