package com.sxhs.realtime.bean;

import lombok.Data;

@Data
public class UploadLogCity {
    private String union_id;
    private String create_time;
    private Long area_id;
    private Integer source;
    private Long id;
    private String collect_location_city;
    private String collect_location_district;
    private Integer upload_number;
    private Integer success_number;
    private Integer fail_number;
    private Integer tube_number;
    private Integer problem_number;
    private Integer problem_number_report;
    private Integer problem_repeat;
    private Integer problem_error;
    private Integer problem_unsync;
    private Integer fix_number;
    private Integer fix_number_report;
    private String upload_time;
    private Integer upload_result;
    private String create_by;
    private Integer is_delete;
}
