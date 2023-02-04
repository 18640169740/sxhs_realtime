package com.sxhs.realtime.bean;

import lombok.Data;

/**
 * @Description: nuc_db.upload_log_fail
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 18:33
 */
@Data
public class UploadLogFail {
    private String upload_time;
    private long area_id;
    private int source;
    private long id;
    private String number_report;
    private int upload_number;
    private int success_number;
    private int fail_number;
    private String data_file;
    private String section_time;
    private int upload_result;
    private String create_by;
    private String create_time;
    private int is_delete;
}
