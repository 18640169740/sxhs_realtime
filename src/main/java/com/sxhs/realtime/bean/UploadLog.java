package com.sxhs.realtime.bean;

import lombok.Data;

/**
 * @Description: nuc_db.upload_log or upload_log_fail
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 18:32
 */
@Data
public class UploadLog {
    private String upload_time;
    private Long area_id;
    private Integer source;
    private Long id;
    private String number_report;
    private int upload_number = 0;
    private int success_number = 0;
    private int fail_number = 0;
    private String data_file;
    private String section_time;
    private int upload_result;
    private String create_by;
    private String create_time;
    private int is_delete;
}
