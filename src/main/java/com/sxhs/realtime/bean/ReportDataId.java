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

    private Integer numberReport;
    private String interfaceRecTime;
    private String userName;
    private Long clientId;
    private Integer dropDataNum;
}
