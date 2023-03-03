package bean;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StandardizedField {
    private String voltage;
    private String current;
    private String stepTime;
    private String totTime;
    private String stepNo;
    private String power;
    private String soc;
    private String type;
    private String curCycle;
    private String totCycle;
    private String capaSum;
    private String charCapAh;
    private String discharCapAh;
    private String chargeWh;
    private String disChargeWh;
    private String totalEnergyWh;
    private String startTime;
    private String endTime;
    private String workingDate;
    private String workingTime;
    private String cellvoltMAX;
    private String cellvoltMIN;
    private String temperatureMAX;
    private String temperatureMIN;
    private String batAvgCelVol;
    private String batAvgTemp;
    private String diffVol;
}
