package com.sxhs.realtime.bean;

import lombok.Data;


@Data
public class TransportDataId {
    private Long id;
    private Long submitId;
    private Long areaId;
    private String deliveryCode;
    private String deliveryName;
    private String deliveryTime;
    private String packTime;
    private String collectAddress;
    private Integer deliveryStatus;
    private String deliveryOrgName;
    private String deliveryPrp;
    private String transportOrgName;
    private String transportPrp;
    private String receiveOrgName;
    private Integer receiveOrgType;
    private String receivePrp;
    private String addTime;
    private Integer tubeNum;
    private Integer packNum;
    private TransportItem[] transportItem;

    private Integer numberReport;
    private String interfaceRecTime;

}
