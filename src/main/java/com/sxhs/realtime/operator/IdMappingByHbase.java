package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CommonDuplicateData;
import com.sxhs.realtime.common.Constants;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * @Description: 通过hbase进行ID去重
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/28 14:59
 */
public class IdMappingByHbase extends KeyedProcessFunction<String, CommonDuplicateData, CommonDuplicateData> {

    //hbase相关
    private Connection connection = null;
    //id去重表
    private Table idTable;
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

        idTable = connection.getTable(TableName.valueOf(parameterTool.getRequired("hbase.id.table")));
        relationTable = connection.getTable(TableName.valueOf(parameterTool.getRequired("hbase.relation.table")));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (idTable != null){
            idTable.close();
        }
        if (relationTable != null){
            relationTable.close();
        }
        if (connection != null){
            connection.close();
        }
    }

    @Override
    public void processElement(CommonDuplicateData commonDuplicateData, Context context, Collector<CommonDuplicateData> collector) throws Exception {
        String type = commonDuplicateData.getType();
        JSONArray jsonArray = commonDuplicateData.getData();
        switch (type){
            case Constants.COLLECT_DATA:
                //id维护
                commonDataProcess(jsonArray, Constants.COLLECT_DATA, Arrays.asList("personIdCard","collectTime"));
                //将采样数据中person_id_card+tube_code存入到hbase缓存中
                collectRelProcess(jsonArray);
                break;
            case Constants.TRANSPORT_DATA:
                transportReceiveProcess(jsonArray, "deliveryCode", Constants.TRANSPORT_DATA, "transportItem", Constants.TRANSPORT_TUBE);
                transportRelProcess(jsonArray);
                break;
            case Constants.RECEIVE_DATA:
                transportReceiveProcess(jsonArray, "receiveCode", Constants.RECEIVE_DATA, "receivesItem", Constants.RECEIVE_TUBE);
                receiveRelProcess(jsonArray);
                break;
            case Constants.REPORT_DATA:
                commonDataProcess(jsonArray, Constants.REPORT_DATA, Arrays.asList("personIdCard","collectTime","checkTime"));
                //将检测结果数据中person_id_card+tube_code存入到hbase缓存中
                reportRelProcess(jsonArray);
                //核酸检测结果数据处理
                resultDataProcess(jsonArray);
                break;
            case Constants.XA_REPORT_DATA:
                commonDataProcess(jsonArray, Constants.XA_REPORT_DATA,Arrays.asList("personIdCard", "collectTime", "checkTime"));
                //将检测结果数据中person_id_card+tube_code存入到hbase缓存中
                reportRelProcess(jsonArray);
                //核酸检测结果数据处理
                resultDataProcess(jsonArray);
                break;
            case Constants.CHECK_ORG:
                commonDataProcess(jsonArray, Constants.CHECK_ORG,Arrays.asList("creditCode", "areaId", "orgName"));
                break;
            case Constants.HOUR_SUM_REPORT:
                //commonDataProcess(jsonArray, Constants.HOUR_SUM_REPORT,Arrays.asList("areaId", "collectDate", "unitHour"));
                break;
            default:
                break;
        }
        collector.collect(commonDuplicateData);
    }

    /**
     * 接收数据关联关系存储
     * @param jsonArray
     * @throws IOException
     */
    private void receiveRelProcess(JSONArray jsonArray) throws IOException {
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            String receiveTime = jsonObject.getString("receiveTime");
            String deliveryCode = jsonObject.getString("deliveryCode");
            //hbase key
            byte[] rowKey = deliveryCode.getBytes();
            Put hbaseRelPut = new Put(rowKey);
            hbaseRelPut.addColumn(Constants.HBASE_FAMILY, "receiveTime".getBytes(), receiveTime.getBytes());
            relationTable.put(hbaseRelPut);
        }
    }

    /**
     * 转运数据关联关系存储
     * @param jsonArray
     * @throws IOException
     */
    private void transportRelProcess(JSONArray jsonArray) throws IOException {
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            String tubeNum = jsonObject.getString("tubeNum");
            String packNum = jsonObject.getString("packNum");
            String deliveryTime = jsonObject.getString("deliveryTime");
            String deliveryCode = jsonObject.getString("deliveryCode");
            //hbase key
            byte[] rowKey = deliveryCode.getBytes();
            Put hbaseRelPut = new Put(rowKey);
            hbaseRelPut.addColumn(Constants.HBASE_FAMILY, "tubeNum".getBytes(), tubeNum.getBytes());
            hbaseRelPut.addColumn(Constants.HBASE_FAMILY, "packNum".getBytes(), packNum.getBytes());
            hbaseRelPut.addColumn(Constants.HBASE_FAMILY, "deliveryTime".getBytes(), deliveryTime.getBytes());
            relationTable.put(hbaseRelPut);
        }
    }

    /**
     * 将采样数据中person_id_card+tube_code存入到hbase缓存中
     * @param jsonArray
     * @throws IOException
     */
    private void collectRelProcess(JSONArray jsonArray) throws IOException {
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
            sj.add(jsonObject.getString("personIdCard"));
            sj.add(jsonObject.getString("tubeCode"));
            //hbase key
            String dataKey = sj.toString();
            byte[] rowKey = dataKey.getBytes();
            Put hbaseRelPut = new Put(rowKey);
            hbaseRelPut.addColumn(Constants.HBASE_FAMILY, "collectTime".getBytes(), jsonObject.getString("collectTime").getBytes());
            relationTable.put(hbaseRelPut);

            Put hbaseRelPut2 = new Put(jsonObject.getString("tubeCode").getBytes());
            hbaseRelPut2.addColumn(Constants.HBASE_FAMILY, "tubeCollectTime".getBytes(), jsonObject.getString("collectTime").getBytes());
            hbaseRelPut2.addColumn(Constants.HBASE_FAMILY, "submitId".getBytes(), jsonObject.getString("submitId").getBytes());
            relationTable.put(hbaseRelPut2);
        }
    }

    /**
     * 将检测结果数据中person_id_card+tube_code存入到hbase缓存中
     * @param jsonArray
     * @throws IOException
     */
    private void reportRelProcess(JSONArray jsonArray) throws IOException {
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
            sj.add(jsonObject.getString("personIdCard"));
            sj.add(jsonObject.getString("tubeCode"));
            //hbase key
            String dataKey = sj.toString();
            byte[] rowKey = dataKey.getBytes();
            Put hbaseRelPut = new Put(rowKey);
            hbaseRelPut.addColumn(Constants.HBASE_FAMILY, "checkTime".getBytes(), jsonObject.getString("checkTime").getBytes());
            relationTable.put(hbaseRelPut);
        }
    }

    /**
     * 通用数据id维护
     * @param jsonArray
     * @param type
     * @param paramList
     * @throws IOException
     */
    private void commonDataProcess(JSONArray jsonArray, String type, List<String> paramList) throws IOException {
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
            paramList.forEach(p -> {
                sj.add(jsonObject.getString(p));
            });
            //hbase key
            String dataKey = sj.toString();
            String dataId = getIdByHbase(dataKey, type);
            if (StringUtils.isBlank(dataId)) {
                dataId = SnowflakeIdWorker.generateIdReverse();
                putIdToHbase(dataKey, type, dataId);
            }
            jsonObject.put(Constants.ID, Long.valueOf(dataId));
        }
    }

    /**
     * 核酸检测结果数据处理
     * @param jsonArray
     * @throws IOException
     */
    private void resultDataProcess(JSONArray jsonArray) throws IOException {
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            resultDataProcess(jsonObject,"checkResult",jsonObject.getString("checkResult"));
            resultDataProcess(jsonObject,"iggResult",jsonObject.getString("iggResult"));
            resultDataProcess(jsonObject,"igmResult",jsonObject.getString("igmResult"));
        }
    }

    private void resultDataProcess(JSONObject jsonObject, String key, String checkResult){
        if(StringUtils.isBlank(checkResult)){
            return;
        }
        switch (checkResult){
            case "1":
                jsonObject.put(key,"阴性");
                break;
            case "2":
                jsonObject.put(key,"阳性");
                break;
            default:
                break;
        }
    }

    /**
     * 接收、转运数据ID维护
     * @param jsonArray
     * @param receiveCode2
     * @param receiveData
     * @param receivesItem2
     * @param receiveTube
     * @throws IOException
     */
    private void transportReceiveProcess(JSONArray jsonArray, String receiveCode2, String receiveData, String receivesItem2, String receiveTube) throws IOException {
        for (Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            String receiveCode = jsonObject.getString(receiveCode2);
            //hbase key
            String dataKey = receiveCode;
            String dataId = getIdByHbase(dataKey, receiveData);
            if (StringUtils.isBlank(dataId)) {
                dataId = SnowflakeIdWorker.generateIdReverse();
                putIdToHbase(dataKey, receiveData, dataId);
            }
            jsonObject.put(Constants.ID, Long.valueOf(dataId));

            JSONArray receivesItem = jsonObject.getJSONArray(receivesItem2);
            for (Object itemObj : receivesItem) {
                JSONObject itemJsonObject = (JSONObject) itemObj;
                String tubeCode = itemJsonObject.getString("tubeCode");
                //hbase key
                String dataKey2 = tubeCode;
                String dataId2 = getIdByHbase(dataKey2, receiveTube);
                if (StringUtils.isBlank(dataId2)) {
                    dataId2 = SnowflakeIdWorker.generateIdReverse();
                    putIdToHbase(dataKey2, receiveTube, dataId2);
                }
                itemJsonObject.put(Constants.ID, Long.valueOf(dataId2));
            }
        }
    }

    /**
     * 将生成的id插入hbase
     *
     * @param key
     * @param value
     * @throws IOException
     */
    private void putIdToHbase(String key, String col, String value) throws IOException {
        byte[] rowKey = MD5Hash.getMD5AsHex((key).getBytes()).getBytes();
        Put hbaseRelPut = new Put(rowKey);
        hbaseRelPut.addColumn(Constants.HBASE_FAMILY, col.getBytes(), value.getBytes());
        idTable.put(hbaseRelPut);
    }

    /**
     * 通过hbase获取id
     *
     * @param key
     * @return
     * @throws IOException
     */
    private String getIdByHbase(String key,String col) throws IOException {
        String id = null;
        //用户表
        byte[] deviceRowKey = MD5Hash.getMD5AsHex((key).getBytes()).getBytes();
        //从hbase拉取数据
        Get get = new Get(deviceRowKey);
        Result result = idTable.get(get);
        if (result != null && result.containsColumn(Constants.HBASE_FAMILY, col.getBytes())) {
            id = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, col.getBytes()));
        }
        return id;
    }
}
