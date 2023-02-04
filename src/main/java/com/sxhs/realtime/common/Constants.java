package com.sxhs.realtime.common;

import com.sxhs.realtime.bean.ProblemDataCr;
import com.sxhs.realtime.bean.ProblemDataTre;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.util.OutputTag;

/**
 * @Description:
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/16 14:55
 */
public class Constants {
    /**
     * hbase相关
     */
    //hbase字段分隔符
    public static final String HBASE_KEY_SPLIT = "_";
    //列族
    public static byte[] HBASE_FAMILY = "cf".getBytes();
    //列
    public static byte[] HBASE_COLUMN = "col".getBytes();

    /**
     * 核酸数据类型
     */
    public static final String COLLECT_DATA = "COLLECT_DATA";
    public static final String TRANSPORT_DATA = "TRANSPORT_DATA";
    public static final String RECEIVE_DATA = "RECEIVE_DATA";
    public static final String REPORT_DATA = "REPORT_DATA";
    public static final String XA_REPORT_DATA = "XA_REPORT_DATA";
    public static final String CHECK_ORG = "CHECK_ORG";
    public static final String HOUR_SUM_REPORT = "HOUR_SUM_REPORT";
    public static final String TRANSPORT_TUBE = "TRANSPORT_TUBE";
    public static final String RECEIVE_TUBE = "RECEIVE_TUBE";

    /**
     * 数据字段
     */
    public static final String ID = "id";

    /**
     * 通用常量
     */
    public static FastDateFormat FASTDATEFORMAT= FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    /**
     * 工单问题类型
     */
    public static final String UPLOAD_NUMBER = "upload_number";
    public static final String SUCCESS_NUMBER = "success_number";
    public static final String FAIL_NUMBER = "fail_number";
    public static final String FAIL_END_NUMBER = "fail_end_number";
    public static final String ERROR_NUMBER = "error_number";
    public static final String CID_NUMBER = "cid_number";
    public static final String CNAME_NUMBER = "cname_number";
    public static final String CPHONE_NUMBER = "cphone_number";
    public static final String RID_NUMBER = "rid_number";
    public static final String RNAME_NUMBER = "rname_number";
    public static final String RPHONE_NUMBER = "rphone_number";
    public static final String TIME_NUMBER = "time_number";
    public static final String TR_ID_NUMBER = "tr_id_number";
    public static final String TR_PHONE_NUMBER = "tr_phone_number";
    public static final String RE_ID_NUMBER = "re_id_number";
    public static final String RE_PHONE_NUMBER = "re_phone_number";

    /**
     * flink OutputTag
     */
    public static OutputTag<ProblemDataCr> CR_TAG = new OutputTag<ProblemDataCr>("crTag") {};
    public static OutputTag<ProblemDataTre> TRE_TAG = new OutputTag<ProblemDataTre>("treTag") {};

    public static OutputTag<String> UPLOAD_LOG_TAG = new OutputTag<String>("uploadLogTag") {};
    public static OutputTag<String> UPLOAD_LOG_FAIL_TAG = new OutputTag<String>("uploadLogFailTag") {};
    public static OutputTag<String> UPLOAD_REPORT_TAG = new OutputTag<String>("uploadReportTag") {};
    public static OutputTag<String> UPLOAD_REPORT_FAIL_TAG = new OutputTag<String>("uploadReportFailTag") {};
}
