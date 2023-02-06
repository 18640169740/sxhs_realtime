package com.sxhs.realtime.bean;

import lombok.Data;

@Data
public class ReceiveDataId {
    private Long id;
    private Long submitId;
    private Long areaId;
    private String receiveCode;
    private String receiveName;
    private String receiveTime;
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
    private String addTime;
    private Integer tubeNum;
    private Integer packNum;
    private TransportItem[] receivesItem;
    private Integer numberReport;
    private String interfaceRecTime;
    private String userName;
    private Long clientId;
    private Integer dropDataNum;

}
