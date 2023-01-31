package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.HourSumReportId;
import com.sxhs.realtime.bean.ProblemData;
import com.sxhs.realtime.bean.UploadLogPro;
import com.sxhs.realtime.common.Constants;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import org.apache.commons.lang3.StringUtils;
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
import java.util.StringJoiner;

/**
 * @Description: 统计当天内记录的上报与人次数据
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/31 13:53
 */
public class ProblemDataStat extends KeyedProcessFunction<Tuple3<String,String,String>, ProblemData, String> {

    //hbase相关
    private Connection connection = null;
    //统计缓存表
    private Table uploadLogTable;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", parameterTool.getRequired("hbase.zookeeper.quorum"));
        configuration.set("zookeeper.znode.parent", parameterTool.getRequired("zookeeper.znode.parent"));
        connection = ConnectionFactory.createConnection(configuration);
        uploadLogTable = connection.getTable(TableName.valueOf(parameterTool.getRequired("hbase.upload.log.table")));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (uploadLogTable != null){
            uploadLogTable.close();
        }
        if (connection != null){
            connection.close();
        }
    }

    @Override
    public void processElement(ProblemData problemData, Context context, Collector<String> collector) throws Exception {
        Long areaId = problemData.getArea_id();
        Integer source = problemData.getSource();
        String createTime = problemData.getCreate_time();
        if(areaId == null || source == null || StringUtils.isBlank(createTime)){
            return;
        }

        StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
        sj.add(areaId.toString());
        sj.add(source.toString());
        sj.add(createTime.substring(0,10));

        byte[] rowKey = sj.toString().getBytes();

        //从hbase拉取数据
        Get get = new Get(rowKey);
        Result result = uploadLogTable.get(get);
        Put hbasePut = new Put(rowKey);

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

        //统计数据
        if (result == null) {
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
        }else{
            id = Bytes.toLong(result.getValue(Constants.HBASE_FAMILY, "id".getBytes()));
            upload_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "upload_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.UPLOAD_NUMBER);
            success_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "success_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.SUCCESS_NUMBER);
            fail_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "fail_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.FAIL_NUMBER);
            fail_end_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "fail_end_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.FAIL_END_NUMBER);
            error_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "error_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.ERROR_NUMBER);
            cid_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "cid_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.CID_NUMBER);
            cname_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "cname_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.CNAME_NUMBER);
            cphone_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "cphone_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.CPHONE_NUMBER);
            rid_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "rid_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.RID_NUMBER);
            rname_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "rname_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.RNAME_NUMBER);
            rphone_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "rphone_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.RPHONE_NUMBER);
            time_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "time_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.TIME_NUMBER);
            tr_id_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "tr_id_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.TR_ID_NUMBER);
            tr_phone_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "tr_phone_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.TR_PHONE_NUMBER);
            re_id_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "re_id_number".getBytes()))
                + getUploadLogNumByType(problemData,Constants.RE_ID_NUMBER);
            re_phone_number = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "re_phone_number".getBytes()))
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
        uploadLogPro.setCreate_time(Constants.FASTDATEFORMAT.format(new Date()));
        uploadLogPro.setIs_delete(0);
        uploadLogPro.setTr_id_number(tr_id_number);
        uploadLogPro.setTr_phone_number(tr_phone_number);
        uploadLogPro.setRe_id_number(re_id_number);
        uploadLogPro.setRe_phone_number(re_phone_number);

        //统计结果更新
        hbasePut.addColumn(Constants.HBASE_FAMILY, "id".getBytes(), id.toString().getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "upload_number".getBytes(),String.valueOf(upload_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "success_number".getBytes(), String.valueOf(success_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "fail_number".getBytes(), String.valueOf(fail_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "fail_end_number".getBytes(), String.valueOf(fail_end_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "error_number".getBytes(), String.valueOf(error_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "cid_number".getBytes(), String.valueOf(cid_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "cname_number".getBytes(), String.valueOf(cname_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "cphone_number".getBytes(), String.valueOf(cphone_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "rid_number".getBytes(), String.valueOf(rid_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "rname_number".getBytes(), String.valueOf(rname_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "rphone_number".getBytes(), String.valueOf(rphone_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "time_number".getBytes(), String.valueOf(time_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "tr_id_number".getBytes(), String.valueOf(tr_id_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "tr_phone_number".getBytes(), String.valueOf(tr_phone_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "re_id_number".getBytes(), String.valueOf(re_id_number).getBytes());
        hbasePut.addColumn(Constants.HBASE_FAMILY, "re_phone_number".getBytes(), String.valueOf(re_phone_number).getBytes());
        uploadLogTable.put(hbasePut);
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
