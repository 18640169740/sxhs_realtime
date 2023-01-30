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
    //hbase
    public static byte[] HBASE_FAMILY = "cf".getBytes();
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

    public static FastDateFormat FASTDATEFORMAT= FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    public static OutputTag<ProblemDataCr> CR_TAG = new OutputTag<ProblemDataCr>("crTag") {};
    public static OutputTag<ProblemDataTre> TRE_TAG = new OutputTag<ProblemDataTre>("treTag") {};
}
