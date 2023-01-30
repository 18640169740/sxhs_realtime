package com.sxhs.realtime.bean;

import lombok.Data;

import java.util.Date;

/**
 * @Description: 转运工单表
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/29 11:15
 */
@Data
public class ProblemDataTre {
    private String add_time;
    private Long area_id;
    private Integer source;
    private Long id;
    private Long submit_id;
    private String code;
    private String name;
    private String time;
    private String number_report;
    private Integer type;
    private String problem_type;
    private String problem_record;
    private Integer is_delete;
    private Integer is_valid;
    private String create_by;
    private String create_time;
    private String update_by;
    private String update_time;
    private String remark;
}
