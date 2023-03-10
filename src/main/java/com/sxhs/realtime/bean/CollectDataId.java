package com.sxhs.realtime.bean;

import lombok.Data;

@Data
public class CollectDataId {
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
    private String collectOrgName;
    private String personId;
    private Integer collectTypeId;
    private Integer collectPartId;
    private String tubeCode;
    private String collectUser;
    private String collectTime;
    private String addTime;
    private Integer collectCount;
    private Integer collectLimitnum;

    // 统计计算用，生成id时需要保存下来，原始数据为String，转int
    private Long numberReport;
    private String interfaceRecTime;
    private String userName;
    private Long clientId;
    private Integer dropDataNum;

    public CollectDataId() {
    }

    public CollectDataId(String personName, String personPhone, String personIdCard, Integer collectCount, Integer collectLimitnum, String addTime, String collectTime, String tubeCode) {
        this.personName = personName;
        this.personPhone = personPhone;
        this.personIdCard = personIdCard;
        this.collectCount = collectCount;
        this.collectLimitnum = collectLimitnum;
        this.addTime = addTime;
        this.collectTime = collectTime;
        this.tubeCode = tubeCode;
    }
}
