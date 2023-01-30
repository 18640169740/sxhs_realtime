package com.sxhs.realtime.bean;

import com.alibaba.fastjson.JSONArray;
import lombok.Data;

/**
 * @Description: 通用任务去重bean
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/28 11:21
 */
@Data
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

}
