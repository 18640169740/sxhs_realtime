package com.sxhs.realtime.bean;

import com.alibaba.fastjson.JSONArray;

/**
 * @Description: 通用任务去重bean
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/28 11:21
 */
public class CommonDuplicateData {

    //上报人名称
    private String userName;
    //数据类型
    private String type;
    //上报客户端id
    private String clientId;
    //上报批次号
    private Long numberReport;
    //数据接收时间
    private String interfaceRecTime;
    //一次校验抛弃数据
    private Long dropDataNum;
    //各上报端消息内容
    private JSONArray data;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Long getNumberReport() {
        return numberReport;
    }

    public void setNumberReport(Long numberReport) {
        this.numberReport = numberReport;
    }

    public String getInterfaceRecTime() {
        return interfaceRecTime;
    }

    public void setInterfaceRecTime(String interfaceRecTime) {
        this.interfaceRecTime = interfaceRecTime;
    }

    public Long getDropDataNum() {
        return dropDataNum;
    }

    public void setDropDataNum(Long dropDataNum) {
        this.dropDataNum = dropDataNum;
    }

    public JSONArray getData() {
        return data;
    }

    public void setData(JSONArray data) {
        this.data = data;
    }
}
