package com.sxhs.realtime.bean;

import lombok.Data;

import java.util.Date;

/**
 * @Description: 采检工单表
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/29 11:05
 */
@Data
public class ProblemDataCr {
    private String add_time;
    private Long area_id;
    private Integer source;
    private Long id;
    private Long submit_id;
    private String person_name;
    private String person_id_card;
    private String number_report;
    private String collect_time;
    private String check_time;
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
