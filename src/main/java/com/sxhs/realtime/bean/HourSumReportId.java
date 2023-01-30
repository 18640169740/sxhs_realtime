package com.sxhs.realtime.bean;

import lombok.Data;

import java.util.Date;
@Data
public class HourSumReportId {
    private String createTime;
    private Long areaId;
    private Long id;
    private String collectDate;
    private int unitHour;
    private int collectPersons;
    private int collectSampleNum;
    private int transferPersons;
    private int transferSampleNum;
    private int receivePersons;
    private int receiveSampleNum;
    private int checkPersons;
    private int checkSampleNum;
    private String updateTime;
    private int isDelete;
    private String createBy;

}
