package com.sxhs.realtime.bean;

import java.util.Date;

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
    private Date collectTime;
    private Date addTime;
    private Integer collectCount;
    private Integer collectLimitnum;

    // 统计计算用，生成id时需要保存下来
    private String numberReport;
    private Date interfaceRecTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getSubmitId() {
        return submitId;
    }

    public void setSubmitId(Long submitId) {
        this.submitId = submitId;
    }

    public Long getAreaId() {
        return areaId;
    }

    public void setAreaId(Long areaId) {
        this.areaId = areaId;
    }

    public Long getRecordId() {
        return recordId;
    }

    public void setRecordId(Long recordId) {
        this.recordId = recordId;
    }

    public String getPersonIdCard() {
        return personIdCard;
    }

    public void setPersonIdCard(String personIdCard) {
        this.personIdCard = personIdCard;
    }

    public String getPersonIdCardType() {
        return personIdCardType;
    }

    public void setPersonIdCardType(String personIdCardType) {
        this.personIdCardType = personIdCardType;
    }

    public String getPersonName() {
        return personName;
    }

    public void setPersonName(String personName) {
        this.personName = personName;
    }

    public String getPersonPhone() {
        return personPhone;
    }

    public void setPersonPhone(String personPhone) {
        this.personPhone = personPhone;
    }

    public String getCollectLocationName() {
        return collectLocationName;
    }

    public void setCollectLocationName(String collectLocationName) {
        this.collectLocationName = collectLocationName;
    }

    public String getCollectLocationProvince() {
        return collectLocationProvince;
    }

    public void setCollectLocationProvince(String collectLocationProvince) {
        this.collectLocationProvince = collectLocationProvince;
    }

    public String getCollectLocationCity() {
        return collectLocationCity;
    }

    public void setCollectLocationCity(String collectLocationCity) {
        this.collectLocationCity = collectLocationCity;
    }

    public String getCollectLocationDistrict() {
        return collectLocationDistrict;
    }

    public void setCollectLocationDistrict(String collectLocationDistrict) {
        this.collectLocationDistrict = collectLocationDistrict;
    }

    public String getCollectLocationStreet() {
        return collectLocationStreet;
    }

    public void setCollectLocationStreet(String collectLocationStreet) {
        this.collectLocationStreet = collectLocationStreet;
    }

    public String getCollectOrgName() {
        return collectOrgName;
    }

    public void setCollectOrgName(String collectOrgName) {
        this.collectOrgName = collectOrgName;
    }

    public String getPersonId() {
        return personId;
    }

    public void setPersonId(String personId) {
        this.personId = personId;
    }

    public Integer getCollectTypeId() {
        return collectTypeId;
    }

    public void setCollectTypeId(Integer collectTypeId) {
        this.collectTypeId = collectTypeId;
    }

    public Integer getCollectPartId() {
        return collectPartId;
    }

    public void setCollectPartId(Integer collectPartId) {
        this.collectPartId = collectPartId;
    }

    public String getTubeCode() {
        return tubeCode;
    }

    public void setTubeCode(String tubeCode) {
        this.tubeCode = tubeCode;
    }

    public String getCollectUser() {
        return collectUser;
    }

    public void setCollectUser(String collectUser) {
        this.collectUser = collectUser;
    }

    public Date getCollectTime() {
        return collectTime;
    }

    public void setCollectTime(Date collectTime) {
        this.collectTime = collectTime;
    }

    public Date getAddTime() {
        return addTime;
    }

    public void setAddTime(Date addTime) {
        this.addTime = addTime;
    }

    public Integer getCollectCount() {
        return collectCount;
    }

    public void setCollectCount(Integer collectCount) {
        this.collectCount = collectCount;
    }

    public Integer getCollectLimitnum() {
        return collectLimitnum;
    }

    public void setCollectLimitnum(Integer collectLimitnum) {
        this.collectLimitnum = collectLimitnum;
    }

    public String getNumberReport() {
        return numberReport;
    }

    public void setNumberReport(String numberReport) {
        this.numberReport = numberReport;
    }

    public Date getInterfaceRecTime() {
        return interfaceRecTime;
    }

    public void setInterfaceRecTime(Date interfaceRecTime) {
        this.interfaceRecTime = interfaceRecTime;
    }
}
