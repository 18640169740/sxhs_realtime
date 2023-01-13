package com.sxhs.realtime.bean;

import java.util.Date;

public class HourSumReportId {
    private Long id;
    private Long areaId;
    private Date collectDate;
    private int unitHour;
    private int collectPersons;
    private int collectSampleNum;
    private int transferPersons;
    private int transferSampleNum;
    private int receivePersons;
    private int receiveSampleNum;
    private int checkPersons;
    private int checkSampleNum;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getAreaId() {
        return areaId;
    }

    public void setAreaId(Long areaId) {
        this.areaId = areaId;
    }

    public Date getCollectDate() {
        return collectDate;
    }

    public void setCollectDate(Date collectDate) {
        this.collectDate = collectDate;
    }

    public int getUnitHour() {
        return unitHour;
    }

    public void setUnitHour(int unitHour) {
        this.unitHour = unitHour;
    }

    public int getCollectPersons() {
        return collectPersons;
    }

    public void setCollectPersons(int collectPersons) {
        this.collectPersons = collectPersons;
    }

    public int getCollectSampleNum() {
        return collectSampleNum;
    }

    public void setCollectSampleNum(int collectSampleNum) {
        this.collectSampleNum = collectSampleNum;
    }

    public int getTransferPersons() {
        return transferPersons;
    }

    public void setTransferPersons(int transferPersons) {
        this.transferPersons = transferPersons;
    }

    public int getTransferSampleNum() {
        return transferSampleNum;
    }

    public void setTransferSampleNum(int transferSampleNum) {
        this.transferSampleNum = transferSampleNum;
    }

    public int getReceivePersons() {
        return receivePersons;
    }

    public void setReceivePersons(int receivePersons) {
        this.receivePersons = receivePersons;
    }

    public int getReceiveSampleNum() {
        return receiveSampleNum;
    }

    public void setReceiveSampleNum(int receiveSampleNum) {
        this.receiveSampleNum = receiveSampleNum;
    }

    public int getCheckPersons() {
        return checkPersons;
    }

    public void setCheckPersons(int checkPersons) {
        this.checkPersons = checkPersons;
    }

    public int getCheckSampleNum() {
        return checkSampleNum;
    }

    public void setCheckSampleNum(int checkSampleNum) {
        this.checkSampleNum = checkSampleNum;
    }
}
