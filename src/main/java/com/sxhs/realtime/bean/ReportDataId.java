package com.sxhs.realtime.bean;

import lombok.Data;

@Data
public class ReportDataId {
    private Long id;
    private Long submitId;
    private Long areaId;
    private Long recordId;
    private String personIdCard;
    private String personIdCardType;
    private String personName;
    private String personPhone;
    private String collectLocationName;
    private String collectLocationProvince;
    private String collectLocationCity;
    private String collectLocationDistrict;
    private String collectLocationStreet;
    private Integer collectTypeId;
    private Integer collectPartId;
    private String tubeCode;
    private String collectUser;
    private String collectTime;
    private String packTime;
    private String deliveryCode;
    private String collectOrgName;
    private String checkOrgName;
    private String receiveTime;
    private String receiveUser;
    private String personId;
    private String checkTime;
    private String checkUser;
    private String checkResult;
    private String iggResult;
    private String igmResult;
    private String remark;
    private String addTime;
    private Integer collectLimitnum;
    private Integer collectCount;

    private Long numberReport;
    private String interfaceRecTime;
    private String userName;
    private Long clientId;
    private Integer dropDataNum;

    public ReportDataId() {
    }

    public ReportDataId(String personIdCard, String personName, String personPhone, String packTime, String collectTime, String receiveTime, String checkTime, String addTime, Integer collectCount, Integer collectLimitnum, String tubeCode) {
        this.personIdCard = personIdCard;
        this.personName = personName;
        this.personPhone = personPhone;
        this.packTime = packTime;
        this.collectTime = collectTime;
        this.receiveTime = receiveTime;
        this.checkTime = checkTime;
        this.addTime = addTime;
        this.collectCount = collectCount;
        this.collectLimitnum = collectLimitnum;
        this.tubeCode = tubeCode;
    }
}
