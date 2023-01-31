package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.*;
import com.sxhs.realtime.common.Constants;
import com.sxhs.realtime.util.NucCheckUtil;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.util.Date;
import java.util.List;

/**
 * @Description:  上报日志统计
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 18:21
 */
public class ReportLogProcess extends ProcessFunction<String,String> {

    //hbase相关
    private Connection connection = null;
    //各环节关联关系表
    private Table relationTable;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", parameterTool.getRequired("hbase.zookeeper.quorum"));
        configuration.set("zookeeper.znode.parent", parameterTool.getRequired("zookeeper.znode.parent"));
        connection = ConnectionFactory.createConnection(configuration);
        relationTable = connection.getTable(TableName.valueOf(parameterTool.getRequired("hbase.relation.table")));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (relationTable != null){
            relationTable.close();
        }
        if (connection != null){
            connection.close();
        }
    }

    @Override
    public void processElement(String str, Context context, Collector<String> collector) throws Exception {
        if(StringUtils.isBlank(str)){
            return;
        }
        JSONObject jsonObject = JSONObject.parseObject(str);
        String type = jsonObject.getString("type");
        if(!Constants.COLLECT_DATA.equals(type) && !Constants.TRANSPORT_DATA.equals(type)
                && !Constants.RECEIVE_DATA.equals(type) && !Constants.REPORT_DATA.equals(type) && !Constants.REPORT_DATA.equals(type)){
            return;
        }
        JSONArray data = jsonObject.getJSONArray("data");
        //id
        String id = SnowflakeIdWorker.generateIdReverse();
        //区域编码
        Long areaId = null;
        //上传批次号
        String numberReport = jsonObject.getString("numberReport");
        //上传环节 1采样 2转运 3接收 4结果
        Integer source = null;
        int total = 0;
        int sucessNum = 0;
        int failNum =0;
        String uploadTime = "";
        String sectionTime = "";
        String createBy = jsonObject.getString("userName");
        //批次号
        String recordId = "";
        //批次内接收到的流水号
        String submitId = "";
        for (Object obj : data) {
            JSONObject object = (JSONObject) obj;
            total ++;
            switch (type){
                case Constants.COLLECT_DATA:
                    CollectDataId collectDataId = JSON.parseObject(object.toJSONString(), CollectDataId.class);
                    Tuple2<List<ProblemDataCr>, List<ProblemDataCr>> collectResult = NucCheckUtil.collectCheck(collectDataId, relationTable);
                    List<ProblemDataCr> collectErrorList = collectResult.f0;
                    if(collectErrorList.size() > 0){
                        failNum ++;
                    }else{
                        sucessNum ++;
                    }
                    source = 1;
                    areaId = collectDataId.getAreaId();
                    uploadTime = collectDataId.getAddTime();
                    sectionTime = collectDataId.getCollectTime();
                    recordId = collectDataId.getRecordId().toString();
                    submitId = collectDataId.getSubmitId().toString();
                    break;
                case Constants.TRANSPORT_DATA:
                    TransportDataId transportDataId = JSON.parseObject(object.toJSONString(), TransportDataId.class);
                    Tuple2<List<ProblemDataTre>, List<ProblemDataTre>> transportResult = NucCheckUtil.transportCheck(transportDataId, relationTable);
                    List<ProblemDataTre> transportErrorList = transportResult.f0;
                    if(transportErrorList.size() > 0){
                        failNum ++;
                    }else{
                        sucessNum ++;
                    }
                    source = 2;
                    areaId = transportDataId.getAreaId();
                    uploadTime = transportDataId.getAddTime();
                    sectionTime = transportDataId.getDeliveryTime();
                    submitId = transportDataId.getSubmitId().toString();
                    break;
                case Constants.RECEIVE_DATA:
                    ReceiveDataId receiveDataId = JSON.parseObject(object.toJSONString(), ReceiveDataId.class);
                    Tuple2<List<ProblemDataTre>, List<ProblemDataTre>> receiveResult = NucCheckUtil.receiveCheck(receiveDataId, relationTable);
                    List<ProblemDataTre> receiveErrorList = receiveResult.f0;
                    if(receiveErrorList.size() > 0){
                        failNum ++;
                    }else{
                        sucessNum ++;
                    }
                    source = 3;
                    areaId = receiveDataId.getAreaId();
                    uploadTime = receiveDataId.getAddTime();
                    sectionTime = receiveDataId.getReceiveTime();
                    submitId = receiveDataId.getSubmitId().toString();
                    break;
                case Constants.REPORT_DATA:
                case Constants.XA_REPORT_DATA:
                    ReportDataId reportDataId = JSON.parseObject(object.toJSONString(), ReportDataId.class);
                    Tuple2<List<ProblemDataCr>, List<ProblemDataCr>> checkResult = NucCheckUtil.reportCheck(reportDataId, relationTable);
                    List<ProblemDataCr> checkErrorList = checkResult.f0;
                    if(checkErrorList.size() > 0){
                        failNum ++;
                    }else{
                        sucessNum ++;
                    }
                    source = 4;
                    areaId = reportDataId.getAreaId();
                    uploadTime = reportDataId.getAddTime();
                    sectionTime = reportDataId.getCheckTime();
                    recordId = reportDataId.getRecordId().toString();
                    submitId = reportDataId.getSubmitId().toString();
                    break;
                default:
                    break;
            }

        }
        //采转送检表日志表
        UploadLog uploadLog = new UploadLog();
        uploadLog.setId(Long.parseLong(SnowflakeIdWorker.generateIdReverse()));
        uploadLog.setArea_id(areaId);
        uploadLog.setNumber_report(numberReport);
        uploadLog.setSource(source);
        uploadLog.setUpload_number(total);
        uploadLog.setSuccess_number(sucessNum);
        uploadLog.setFail_number(failNum);
        uploadLog.setUpload_time(uploadTime);
        uploadLog.setSection_time(sectionTime);
        uploadLog.setUpload_result(1);
        uploadLog.setCreate_by(createBy);
        uploadLog.setCreate_time(Constants.FASTDATEFORMAT.format(new Date()));
        uploadLog.setIs_delete(0);

        if(uploadLog.getArea_id() == null || StringUtils.isBlank(uploadLog.getNumber_report())
        || uploadLog.getSource() == null || uploadLog.getUpload_number() == 0 || StringUtils.isBlank(uploadLog.getUpload_time())){
            context.output(Constants.UPLOAD_LOG_FAIL_TAG,JSONObject.toJSONString(uploadLog));
        }else{
            context.output(Constants.UPLOAD_LOG_TAG,JSONObject.toJSONString(uploadLog));
        }

        if(areaId == null || source == null || StringUtils.isBlank(recordId) || StringUtils.isBlank(numberReport)){
            //采转送检对账表错误数据表
            UploadReportFail uploadReportFail = new UploadReportFail();
            uploadReportFail.setId(Long.parseLong(SnowflakeIdWorker.generateIdReverse()));
            uploadReportFail.setArea_id(areaId);
            uploadReportFail.setSource(source);
            uploadReportFail.setRecord_id(recordId);
            uploadReportFail.setNumber_report(numberReport);
            uploadReportFail.setFail_data(str);
            uploadReportFail.setCreate_by(createBy);
            uploadReportFail.setCreate_time(Constants.FASTDATEFORMAT.format(new Date()));
            uploadReportFail.setIs_delete(0);
            context.output(Constants.UPLOAD_REPORT_FAIL_TAG,JSONObject.toJSONString(uploadReportFail));
        }else{
            //采转送检对账表
            UploadReport uploadReport = new UploadReport();
            uploadReport.setId(Long.parseLong(SnowflakeIdWorker.generateIdReverse()));
            uploadReport.setArea_id(areaId);
            uploadReport.setSource(source);
            uploadReport.setRecord_id(recordId);
            uploadReport.setNumber_report(numberReport);
            uploadReport.setUpload_number(total);
            uploadReport.setSuccess_number(sucessNum);
            uploadReport.setConfirm_status(1);
            uploadReport.setUpload_time(uploadTime);
            uploadReport.setSubmit_id(submitId);
            uploadReport.setCreate_by(createBy);
            uploadReport.setCreate_time(Constants.FASTDATEFORMAT.format(new Date()));
            uploadReport.setIs_delete(0);
            context.output(Constants.UPLOAD_REPORT_TAG,JSONObject.toJSONString(uploadReport));
        }
    }
}
