package com.sxhs.realtime.bean;

import lombok.Data;

/**
 * @Description: nuc_db.upload_report
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 18:33
 */
@Data
public class UploadReport {
    private String upload_time;
    private long area_id;
    private int source;
    private long id;
    private String record_id;
    private String number_report;
    private int upload_number;
    private int success_number;
    private int confirm_status;
    private String confirm_time;
    private String submit_id;
    private String create_by;
    private String create_time;
    private int is_delete;
}
