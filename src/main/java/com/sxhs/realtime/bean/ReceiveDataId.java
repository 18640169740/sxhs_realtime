package com.sxhs.realtime.bean;

import java.util.Date;

public class ReceiveDataId {
    private Long id;
    private Long submitId;
    private Long areaId;
    private String receiveCode;
    private String receiveName;
    private Date receiveTime;
    private String deliveryCode;
    private String deliveryOrgName;
    private String deliveryPrp;
    private String deliveryPrpId;
    private String deliveryPrpPhone;
    private String transportOrgName;
    private String transportPrp;
    private String transportPrpId;
    private String transportPrpPhone;
    private String receiveOrgName;
    private Integer receiveOrgType;
    private String receivePrp;
    private Date addTime;
    private String numberReport;
    private Integer tubeNum;
    private Integer packNum;
    private TransportItem[] receivesItem;

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

    public String getReceiveCode() {
        return receiveCode;
    }

    public void setReceiveCode(String receiveCode) {
        this.receiveCode = receiveCode;
    }

    public String getReceiveName() {
        return receiveName;
    }

    public void setReceiveName(String receiveName) {
        this.receiveName = receiveName;
    }

    public Date getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(Date receiveTime) {
        this.receiveTime = receiveTime;
    }

    public String getDeliveryCode() {
        return deliveryCode;
    }

    public void setDeliveryCode(String deliveryCode) {
        this.deliveryCode = deliveryCode;
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

    public String getDeliveryPrpId() {
        return deliveryPrpId;
    }

    public void setDeliveryPrpId(String deliveryPrpId) {
        this.deliveryPrpId = deliveryPrpId;
    }

    public String getDeliveryPrpPhone() {
        return deliveryPrpPhone;
    }

    public void setDeliveryPrpPhone(String deliveryPrpPhone) {
        this.deliveryPrpPhone = deliveryPrpPhone;
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

    public String getTransportPrpId() {
        return transportPrpId;
    }

    public void setTransportPrpId(String transportPrpId) {
        this.transportPrpId = transportPrpId;
    }

    public String getTransportPrpPhone() {
        return transportPrpPhone;
    }

    public void setTransportPrpPhone(String transportPrpPhone) {
        this.transportPrpPhone = transportPrpPhone;
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

    public String getNumberReport() {
        return numberReport;
    }

    public void setNumberReport(String numberReport) {
        this.numberReport = numberReport;
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

    public TransportItem[] getReceivesItem() {
        return receivesItem;
    }

    public void setReceivesItem(TransportItem[] receivesItem) {
        this.receivesItem = receivesItem;
    }
}
