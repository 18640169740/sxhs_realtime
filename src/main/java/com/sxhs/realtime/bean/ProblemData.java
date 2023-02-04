package com.sxhs.realtime.bean;

import lombok.Data;

/**
 * @Description:
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/31 13:49
 */
@Data
public class ProblemData {
    protected String add_time;
    protected Long area_id;
    protected Integer source;
    protected Long id;
    private Long submit_id;
    protected String number_report;
    protected Integer type;
    protected String problem_type;
    protected String problem_record;
    protected Integer is_delete;
    protected Integer is_valid;
    protected String create_by;
    protected String create_time;
    protected String update_by;
    protected String update_time;
    protected String remark;
}
