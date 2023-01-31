package com.sxhs.realtime.bean;

import lombok.Data;

import java.util.Date;

/**
 * @Description: 转运工单表
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/29 11:15
 */
@Data
public class ProblemDataTre extends ProblemData{
    private String code;
    private String name;
    private String time;
}
