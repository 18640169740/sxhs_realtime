package com.sxhs.realtime.bean;

import java.util.Date;

public class TransportDataId {
    private Long id;
    private Long submitId;
    private Long areaId;
    private String deliveryCode;
    private String deliveryName;
    private Date deliveryTime;
    private Date packTime;
    private String collectAddress;
    private Integer deliveryStatus;
    private String deliveryOrgName;
    private String deliveryPrp;
    private String transportOrgName;
    private String transportPrp;
    private String receiveOrgName;
    private Integer receiveOrgType;
    private String receivePrp;
    private Date addTime;
    private Integer tubeNum;
    private Integer packNum;
    private TransportItem[] transportItem;

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

    public String getDeliveryCode() {
        return deliveryCode;
    }

    public void setDeliveryCode(String deliveryCode) {
        this.deliveryCode = deliveryCode;
    }

    public String getDeliveryName() {
        return deliveryName;
    }

    public void setDeliveryName(String deliveryName) {
        this.deliveryName = deliveryName;
    }

    public Date getDeliveryTime() {
        return deliveryTime;
    }

    public void setDeliveryTime(Date deliveryTime) {
        this.deliveryTime = deliveryTime;
    }

    public Date getPackTime() {
        return packTime;
    }

    public void setPackTime(Date packTime) {
        this.packTime = packTime;
    }

    public String getCollectAddress() {
        return collectAddress;
    }

    public void setCollectAddress(String collectAddress) {
        this.collectAddress = collectAddress;
    }

    public Integer getDeliveryStatus() {
        return deliveryStatus;
    }

    public void setDeliveryStatus(Integer deliveryStatus) {
        this.deliveryStatus = deliveryStatus;
    }

    public String getDeliveryOrgName() {
        return deliveryOrgName;
    }

    public void setDeliveryOrgName(String deliveryOrgName) {
        this.deliveryOrgName = deliveryOrgName;
    }

    public String getDeliveryPrp() {
        return deliveryPrp;
    }

    public void setDeliveryPrp(String deliveryPrp) {
        this.deliveryPrp = deliveryPrp;
    }

    public String getTransportOrgName() {
        return transportOrgName;
    }

    public void setTransportOrgName(String transportOrgName) {
        this.transportOrgName = transportOrgName;
    }

    public String getTransportPrp() {
        return transportPrp;
    }

    public void setTransportPrp(String transportPrp) {
        this.transportPrp = transportPrp;
    }

    public String getReceiveOrgName() {
        return receiveOrgName;
    }

    public void setReceiveOrgName(String receiveOrgName) {
        this.receiveOrgName = receiveOrgName;
    }

    public Integer getReceiveOrgType() {
        return receiveOrgType;
    }

    public void setReceiveOrgType(Integer receiveOrgType) {
        this.receiveOrgType = receiveOrgType;
    }

    public String getReceivePrp() {
        return receivePrp;
    }

    public void setReceivePrp(String receivePrp) {
        this.receivePrp = receivePrp;
    }

    public Date getAddTime() {
        return addTime;
    }

    public void setAddTime(Date addTime) {
        this.addTime = addTime;
    }

    public Integer getTubeNum() {
        return tubeNum;
    }

    public void setTubeNum(Integer tubeNum) {
        this.tubeNum = tubeNum;
    }

    public Integer getPackNum() {
        return packNum;
    }

    public void setPackNum(Integer packNum) {
        this.packNum = packNum;
    }

    public TransportItem[] getTransportItem() {
        return transportItem;
    }

    public void setTransportItem(TransportItem[] transportItem) {
        this.transportItem = transportItem;
    }
}
