package com.sxhs.realtime.bean;

import lombok.Data;

import java.util.Date;

/**
 * @Description: 采检工单表
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/29 11:05
 */
@Data
public class ProblemDataCr extends ProblemData{
    private String person_name;
    private String person_id_card;
    private String collect_time;
    private String check_time;
}
