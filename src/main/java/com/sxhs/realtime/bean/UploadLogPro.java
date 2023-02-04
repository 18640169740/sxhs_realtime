package com.sxhs.realtime.bean;

import lombok.Data;

/**
 * @Description: 当天内记录的上报与人次数据
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/31 14:03
 */
@Data
public class UploadLogPro {
    private Long id;
    private Long area_id;
    private Integer source;
    private Integer upload_number;
    private Integer success_number;
    private Integer fail_number;
    private Integer fail_end_number;
    private Integer error_number;
    private Integer cid_number;
    private Integer cname_number;
    private Integer cphone_number;
    private Integer rid_number;
    private Integer rname_number;
    private Integer rphone_number;
    private Integer time_number;
    private String upload_time;
    private String create_by;
    private String create_time;
    private Integer is_delete;
    private Integer tr_id_number;
    private Integer tr_phone_number;
    private Integer re_id_number;
    private Integer re_phone_number;
}
