package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.*;
import com.sxhs.realtime.common.Constants;
import com.sxhs.realtime.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

/**
 * @Description: 数据校验
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/29 14:50
 */
public class DataCheckProcess<T> extends ProcessFunction<T, Object> {

    //hbase相关
    private Connection connection = null;
    //各环节关联关系表
    private Table relationTable;
    //二次校验缓存表
    private Table checkTable;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", parameterTool.getRequired("hbase.zookeeper.quorum"));
        configuration.set("zookeeper.znode.parent", parameterTool.getRequired("zookeeper.znode.parent"));
        connection = ConnectionFactory.createConnection(configuration);
        relationTable = connection.getTable(TableName.valueOf(parameterTool.getRequired("hbase.relation.table")));
        checkTable = connection.getTable(TableName.valueOf(parameterTool.getRequired("hbase.check.table")));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (relationTable != null){
            relationTable.close();
        }
        if (checkTable != null){
            checkTable.close();
        }
        if (connection != null){
            connection.close();
        }
    }

    @Override
    public void processElement(T t, Context context, Collector<Object> collector) throws Exception {
        if(t instanceof CollectDataId){
            //采样校验
            CollectDataId dto = (CollectDataId) t;
            Tuple2<List<ProblemDataCr>, List<ProblemDataCr>> result = NucCheckUtil.collectCheck(dto, relationTable);
            collectCheckByHbase(context, Constants.CR_TAG, result);

        }else if(t instanceof TransportDataId){
            //转运校验
            TransportDataId dto = (TransportDataId) t;
            Tuple2<List<ProblemDataTre>, List<ProblemDataTre>> result = NucCheckUtil.transportCheck(dto, relationTable);
            treCheckByHbase(context, Constants.TRE_TAG, result);

        }else if(t instanceof ReceiveDataId){
            //接收校验
            ReceiveDataId dto = (ReceiveDataId) t;
            Tuple2<List<ProblemDataTre>, List<ProblemDataTre>> result = NucCheckUtil.receiveCheck(dto, relationTable);
            treCheckByHbase(context, Constants.TRE_TAG, result);

        }else if(t instanceof ReportDataId){
            //检测结果校验
            ReportDataId dto = (ReportDataId) t;
            Tuple2<List<ProblemDataCr>, List<ProblemDataCr>> result = NucCheckUtil.reportCheck(dto, relationTable);
            reportCheckByHbase(context, Constants.CR_TAG, result);
        }else{
            return;
        }
    }

    /**
     * 通过hbase更新检测结果工单
     * @param context
     * @param crTag
     * @param result
     * @throws IOException
     */
    public void reportCheckByHbase(Context context, OutputTag<ProblemDataCr> crTag, Tuple2<List<ProblemDataCr>, List<ProblemDataCr>> result) throws IOException {
        List<ProblemDataCr> errorList = result.f0;
        List<ProblemDataCr> sucessList = result.f1;
        List<Put> putList = new ArrayList<>();
        if(errorList != null && errorList.size() > 0){
            for (ProblemDataCr cr : errorList) {
                Integer source = cr.getSource();
                String person_id_card = cr.getPerson_id_card();
                String checkTime = cr.getCheck_time();
                String problem_record = cr.getProblem_record();
                StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                sj.add(source.toString()).add(person_id_card).add(checkTime).add(problem_record);

                relationHbase(putList, cr, sj);
                context.output(crTag, cr);
            };
        }
        if(sucessList != null && sucessList.size() > 0){
            List<Get> getList = new ArrayList<>();
            for (ProblemDataCr cr : sucessList) {
                Integer source = cr.getSource();
                String person_id_card = cr.getPerson_id_card();
                String checkTime = cr.getCheck_time();
                String problem_record = cr.getProblem_record();
                StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                sj.add(source.toString()).add(person_id_card).add(checkTime).add(problem_record);
                Get get = new Get(MD5Hash.getMD5AsHex((sj.toString()).getBytes()).getBytes());
                getList.add(get);
            }
            Result[] results = checkTable.get(getList);
            for (int i = 0; i < results.length; i++) {
                Result r = results[i];
                String id = "";
                String addTime = "";
                if (r != null && r.containsColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN)) {
                    id = Bytes.toString(r.getValue(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN));
                    addTime = Bytes.toString(r.getValue(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN_TIME));
                }
                if(StringUtils.isNotBlank(id)){
                    ProblemDataCr problemDataCr = sucessList.get(i);
                    problemDataCr.setId(Long.parseLong(id));
                    problemDataCr.setAdd_time(addTime);
                    Integer source = problemDataCr.getSource();
                    String person_id_card = problemDataCr.getPerson_id_card();
                    String checkTime = problemDataCr.getCheck_time();
                    String problem_record = problemDataCr.getProblem_record();
                    StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                    sj.add(source.toString()).add(person_id_card).add(checkTime).add(problem_record);
                    Put put = new Put(MD5Hash.getMD5AsHex((sj.toString()).getBytes()).getBytes());
                    put.addColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN, "".getBytes());
                    putList.add(put);
                    context.output(crTag, problemDataCr);
                }
            }
        }
        if(putList.size() > 0){
            checkTable.put(putList);
        }
    }

    /**
     * 通过hbase更新采集工单
     * @param context
     * @param crTag
     * @param result
     * @throws IOException
     */
    public void collectCheckByHbase(Context context, OutputTag<ProblemDataCr> crTag, Tuple2<List<ProblemDataCr>, List<ProblemDataCr>> result) throws Exception {
        List<ProblemDataCr> errorList = result.f0;
        List<ProblemDataCr> sucessList = result.f1;
        List<Put> putList = new ArrayList<>();
        if(errorList != null && errorList.size() > 0){
            for (ProblemDataCr cr : errorList) {
                Integer source = cr.getSource();
                String person_id_card = cr.getPerson_id_card();
                String collect_time = cr.getCollect_time();
                String problem_record = cr.getProblem_record();
                StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                sj.add(source.toString()).add(person_id_card).add(collect_time).add(problem_record);

                relationHbase(putList, cr, sj);
                context.output(crTag, cr);
            };
        }
        if(sucessList != null && sucessList.size() > 0){
            List<Get> getList = new ArrayList<>();
            for (ProblemDataCr cr : sucessList) {
                Integer source = cr.getSource();
                String person_id_card = cr.getPerson_id_card();
                String collect_time = cr.getCollect_time();
                String problem_record = cr.getProblem_record();
                StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                sj.add(source.toString()).add(person_id_card).add(collect_time).add(problem_record);
                Get get = new Get(MD5Hash.getMD5AsHex((sj.toString()).getBytes()).getBytes());
                getList.add(get);
            }
            Result[] results = checkTable.get(getList);
            for (int i = 0; i < results.length; i++) {
                Result r = results[i];
                String id = "";
                String addTime = "";
                if (r != null && r.containsColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN)) {
                    id = Bytes.toString(r.getValue(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN));
                    addTime = Bytes.toString(r.getValue(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN_TIME));
                }
                if(StringUtils.isNotBlank(id)){
                    ProblemDataCr problemDataCr = sucessList.get(i);
                    problemDataCr.setId(Long.parseLong(id));
                    problemDataCr.setAdd_time(addTime);
                    Integer source = problemDataCr.getSource();
                    String person_id_card = problemDataCr.getPerson_id_card();
                    String collect_time = problemDataCr.getCollect_time();
                    String problem_record = problemDataCr.getProblem_record();
                    StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                    sj.add(source.toString()).add(person_id_card).add(collect_time).add(problem_record);
                    Put put = new Put(MD5Hash.getMD5AsHex((sj.toString()).getBytes()).getBytes());
                    put.addColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN, "".getBytes());
                    putList.add(put);
                    context.output(crTag, problemDataCr);
                }
            }
        }
        if(putList.size() > 0){
            checkTable.put(putList);
        }
    }

    /**
     * 通过hbase更新转运工单
     * @param context
     * @param treTag
     * @param result
     * @throws IOException
     */
    public void treCheckByHbase(Context context, OutputTag<ProblemDataTre> treTag, Tuple2<List<ProblemDataTre>, List<ProblemDataTre>> result) throws IOException {
        List<ProblemDataTre> errorList = result.f0;
        List<ProblemDataTre> sucessList = result.f1;
        List<Put> putList = new ArrayList<>();
        if (errorList != null && errorList.size() > 0) {
            for (ProblemDataTre cr : errorList) {
                Integer source = cr.getSource();
                Long submitId = cr.getSubmit_id();
                String code = cr.getCode();
                String problem_record = cr.getProblem_record();
                StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                sj.add(source.toString()).add(submitId.toString()).add(code).add(problem_record);

                relationHbase(putList, cr, sj);
                context.output(treTag, cr);
            };
        }
        if (sucessList != null && sucessList.size() > 0) {
            List<Get> getList = new ArrayList<>();
            for (ProblemDataTre cr : sucessList) {
                Integer source = cr.getSource();
                Long submitId = cr.getSubmit_id();
                String code = cr.getCode();
                String problem_record = cr.getProblem_record();
                StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                sj.add(source.toString()).add(submitId.toString()).add(code).add(problem_record);
                Get get = new Get(MD5Hash.getMD5AsHex((sj.toString()).getBytes()).getBytes());
                getList.add(get);
            }
            Result[] results = checkTable.get(getList);
            for (int i = 0; i < results.length; i++) {
                Result r = results[i];
                String id = "";
                String addTime = "";
                if (r != null && r.containsColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN)) {
                    id = Bytes.toString(r.getValue(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN));
                    addTime = Bytes.toString(r.getValue(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN_TIME));
                }
                if (StringUtils.isNotBlank(id)) {
                    ProblemDataTre problemDataTre = sucessList.get(i);
                    problemDataTre.setId(Long.parseLong(id));
                    problemDataTre.setAdd_time(addTime);
                    Integer source = problemDataTre.getSource();
                    Long submitId = problemDataTre.getSubmit_id();
                    String code = problemDataTre.getCode();
                    String problem_record = problemDataTre.getProblem_record();
                    StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
                    sj.add(source.toString()).add(submitId.toString()).add(code).add(problem_record);
                    Put put = new Put(MD5Hash.getMD5AsHex((sj.toString()).getBytes()).getBytes());
                    put.addColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN, "".getBytes());
                    putList.add(put);
                    context.output(treTag, problemDataTre);
                }
            }
        }
        if (putList.size() > 0) {
            checkTable.put(putList);
        }
    }

    /**
     * 工单数据关联hbase
     * @param putList
     * @param cr
     * @param sj
     * @throws IOException
     */
    public void relationHbase(List<Put> putList, ProblemData cr, StringJoiner sj) throws IOException {
        Get get = new Get(MD5Hash.getMD5AsHex((sj.toString()).getBytes()).getBytes());
        Result hbaseResult = checkTable.get(get);
        Long id = cr.getId();
        String addTime = cr.getAdd_time();
        if (hbaseResult != null && hbaseResult.containsColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN)) {
            String idStr = Bytes.toString(hbaseResult.getValue(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN));
            addTime = Bytes.toString(hbaseResult.getValue(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN_TIME));
            if(StringUtils.isNotBlank(idStr)){
                id = Long.parseLong(idStr);
                cr.setId(id);
                cr.setAdd_time(addTime);
            }
        }else {
            Put put = new Put(MD5Hash.getMD5AsHex((sj.toString()).getBytes()).getBytes());
            put.addColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN, id.toString().getBytes());
            put.addColumn(Constants.HBASE_FAMILY, Constants.HBASE_COLUMN_TIME, addTime.getBytes());
            putList.add(put);
        }
    }

}
