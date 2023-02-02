package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.thrift.TKeysType;
import com.sxhs.realtime.bean.HourSumReportId;
import com.sxhs.realtime.bean.ProblemData;
import com.sxhs.realtime.bean.UploadLogPro;
import com.sxhs.realtime.common.Constants;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @Description: 统计当天内记录的上报与人次数据
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/31 13:53
 */
public class ProblemDataStat extends KeyedProcessFunction<Tuple3<String,String,String>, ProblemData, String> {

    /**
     * state设置
     */
    private transient MapState<String, Map<String,Long>> mapState;
    //state ttl
    private int stateDays = 2;
    //state name
    private String stateName = "problemStatState";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //set state
        MapStateDescriptor<String, Map<String,Long>> descriptor =
                new MapStateDescriptor<>(
                        stateName,
                        Types.STRING,
                        Types.MAP(Types.STRING,Types.LONG));
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(stateDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(ProblemData problemData, Context context, Collector<String> collector) throws Exception {
        Long areaId = problemData.getArea_id();
        Integer source = problemData.getSource();
        String createTime = problemData.getCreate_time();
        if(areaId == null || source == null || StringUtils.isBlank(createTime)){
            return;
        }

        //key
        StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
        sj.add(areaId.toString());
        sj.add(source.toString());
        sj.add(createTime.substring(0,10));

        UploadLogPro uploadLogPro = new UploadLogPro();
        Long id = 0L;
        int upload_number = 0;
        int success_number = 0;
        int fail_number = 0;
        int fail_end_number = 0;
        int error_number = 0;
        int cid_number = 0;
        int cname_number = 0;
        int cphone_number = 0;
        int rid_number = 0;
        int rname_number = 0;
        int rphone_number = 0;
        int time_number = 0;
        int tr_id_number = 0;
        int tr_phone_number = 0;
        int re_id_number = 0;
        int re_phone_number = 0;

        String key = sj.toString();
        Map<String, Long> result = mapState.get(key);
        //统计数据
        if (result == null || result.size() == 0) {
            id = Long.valueOf(SnowflakeIdWorker.generateIdReverse());
            upload_number = getUploadLogNumByType(problemData,Constants.UPLOAD_NUMBER);
            success_number = getUploadLogNumByType(problemData,Constants.SUCCESS_NUMBER);
            fail_number = getUploadLogNumByType(problemData,Constants.FAIL_NUMBER);
            fail_end_number = getUploadLogNumByType(problemData,Constants.FAIL_END_NUMBER);
            error_number = getUploadLogNumByType(problemData,Constants.ERROR_NUMBER);
            cid_number = getUploadLogNumByType(problemData,Constants.CID_NUMBER);
            cname_number = getUploadLogNumByType(problemData,Constants.CNAME_NUMBER);
            cphone_number = getUploadLogNumByType(problemData,Constants.CPHONE_NUMBER);
            rid_number = getUploadLogNumByType(problemData,Constants.RID_NUMBER);
            rname_number = getUploadLogNumByType(problemData,Constants.RNAME_NUMBER);
            rphone_number = getUploadLogNumByType(problemData,Constants.RPHONE_NUMBER);
            time_number = getUploadLogNumByType(problemData,Constants.TIME_NUMBER);
            tr_id_number = getUploadLogNumByType(problemData,Constants.TR_ID_NUMBER);
            tr_phone_number = getUploadLogNumByType(problemData,Constants.TR_PHONE_NUMBER);
            re_id_number = getUploadLogNumByType(problemData,Constants.RE_ID_NUMBER);
            re_phone_number = getUploadLogNumByType(problemData,Constants.RE_PHONE_NUMBER);
            result = new HashMap<>();
        }else{
            id = result.get("id");
            upload_number = result.get("upload_number").intValue()
                    + getUploadLogNumByType(problemData,Constants.UPLOAD_NUMBER);
            success_number = result.get("success_number").intValue()
                + getUploadLogNumByType(problemData,Constants.SUCCESS_NUMBER);
            fail_number = result.get("fail_number").intValue()
                + getUploadLogNumByType(problemData,Constants.FAIL_NUMBER);
            fail_end_number = result.get("fail_end_number").intValue()
                + getUploadLogNumByType(problemData,Constants.FAIL_END_NUMBER);
            error_number = result.get("error_number").intValue()
                + getUploadLogNumByType(problemData,Constants.ERROR_NUMBER);
            cid_number = result.get("cid_number").intValue()
                + getUploadLogNumByType(problemData,Constants.CID_NUMBER);
            cname_number = result.get("cname_number").intValue()
                + getUploadLogNumByType(problemData,Constants.CNAME_NUMBER);
            cphone_number = result.get("cphone_number").intValue()
                + getUploadLogNumByType(problemData,Constants.CPHONE_NUMBER);
            rid_number = result.get("rid_number").intValue()
                + getUploadLogNumByType(problemData,Constants.RID_NUMBER);
            rname_number = result.get("rname_number").intValue()
                + getUploadLogNumByType(problemData,Constants.RNAME_NUMBER);
            rphone_number = result.get("rphone_number").intValue()
                + getUploadLogNumByType(problemData,Constants.RPHONE_NUMBER);
            time_number = result.get("time_number").intValue()
                + getUploadLogNumByType(problemData,Constants.TIME_NUMBER);
            tr_id_number = result.get("tr_id_number").intValue()
                + getUploadLogNumByType(problemData,Constants.TR_ID_NUMBER);
            tr_phone_number = result.get("tr_phone_number").intValue()
                + getUploadLogNumByType(problemData,Constants.TR_PHONE_NUMBER);
            re_id_number = result.get("re_id_number").intValue()
                + getUploadLogNumByType(problemData,Constants.RE_ID_NUMBER);
            re_phone_number = result.get("re_phone_number").intValue()
                    + getUploadLogNumByType(problemData,Constants.RE_PHONE_NUMBER);
        }

        //构造统计结果实体
        uploadLogPro.setId(id);
        uploadLogPro.setArea_id(areaId);
        uploadLogPro.setSource(source);
        uploadLogPro.setUpload_number(upload_number);
        uploadLogPro.setSuccess_number(success_number);
        uploadLogPro.setFail_number(fail_number);
        uploadLogPro.setFail_end_number(fail_end_number);
        uploadLogPro.setError_number(error_number);
        uploadLogPro.setCid_number(cid_number);
        uploadLogPro.setCname_number(cname_number);
        uploadLogPro.setCphone_number(cphone_number);
        uploadLogPro.setRid_number(rid_number);
        uploadLogPro.setRname_number(rname_number);
        uploadLogPro.setRphone_number(rphone_number);
        uploadLogPro.setTime_number(time_number);
        uploadLogPro.setUpload_time(createTime.substring(0,10) + " 00:00:00");
        uploadLogPro.setCreate_by(problemData.getCreate_by());
        uploadLogPro.setCreate_time(createTime.substring(0,10) + " 00:00:00");
        uploadLogPro.setIs_delete(0);
        uploadLogPro.setTr_id_number(tr_id_number);
        uploadLogPro.setTr_phone_number(tr_phone_number);
        uploadLogPro.setRe_id_number(re_id_number);
        uploadLogPro.setRe_phone_number(re_phone_number);

        //统计结果更新
        result.put("id",id);
        result.put("upload_number", (long) upload_number);
        result.put("success_number", (long) success_number);
        result.put("fail_number", (long) fail_number);
        result.put("fail_end_number", (long) fail_end_number);
        result.put("error_number", (long) error_number);
        result.put("cid_number", (long) cid_number);
        result.put("cname_number", (long) cname_number);
        result.put("cphone_number", (long) cphone_number);
        result.put("rid_number", (long) rid_number);
        result.put("rname_number", (long) rname_number);
        result.put("rphone_number", (long) rphone_number);
        result.put("time_number", (long) time_number);
        result.put("tr_id_number", (long) tr_id_number);
        result.put("tr_phone_number", (long) tr_phone_number);
        result.put("re_id_number", (long) re_id_number);
        result.put("re_phone_number", (long) re_phone_number);

        mapState.put(key,result);
        collector.collect(JSONObject.toJSONString(uploadLogPro));
    }

    /**
     * 根据问题类型获取问题数
     * @param problemData
     * @param type
     * @return
     */
    private int getUploadLogNumByType(ProblemData problemData, String type){
        int num = 0;
        switch (type){
            case Constants.UPLOAD_NUMBER:
                num = 1;
                break;
            case Constants.SUCCESS_NUMBER:
                if(problemData.getIs_valid() == 1){
                    num = 1;
                }
                break;
            case Constants.FAIL_NUMBER:
                if(problemData.getIs_valid() == 0){
                    num = 1;
                }
                break;
            case Constants.FAIL_END_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 4
                    && "5".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.ERROR_NUMBER:
                if(problemData.getIs_valid() == 0
                        && "4".compareTo(problemData.getProblem_type()) >= 0){
                    num = 1;
                }
                break;
            case Constants.CID_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 1
                        && "1".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.CNAME_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 1
                        && "2".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.CPHONE_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 1
                        && "3".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.RID_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 4
                        && "1".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.RNAME_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 4
                        && "2".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.RPHONE_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 4
                        && "3".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.TIME_NUMBER:
                if(problemData.getIs_valid() == 0
                        && "6".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.TR_ID_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 3
                        && "1".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.TR_PHONE_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 3
                        && "2".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.RE_ID_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 3
                        && "3".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            case Constants.RE_PHONE_NUMBER:
                if(problemData.getIs_valid() == 0 && problemData.getSource() == 3
                        && "4".equals(problemData.getProblem_type())){
                    num = 1;
                }
                break;
            default:
                break;
        }
        return num;
    }
}
