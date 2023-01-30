package com.sxhs.realtime.bean;

import lombok.Data;

/**
 * @Description: nuc_db.upload_report_fail
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 18:34
 */
@Data
public class UploadReportFail {
    private String create_time;
    private long area_id;
    private int source;
    private long id;
    private String record_id;
    private String number_report;
    private String fail_data;
    private String create_by;
    private int is_delete;
}
