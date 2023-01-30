package com.sxhs.realtime.operator;

import com.sxhs.realtime.bean.CommonDuplicateData;
import com.sxhs.realtime.bean.HourSumReportId;
import com.sxhs.realtime.common.Constants;
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

import java.util.StringJoiner;

/**
 * @Description: 上报统计信息统计
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 17:24
 */
public class ReportStatProcess extends KeyedProcessFunction<Tuple3<String,String,String>, HourSumReportId, HourSumReportId> {

    //hbase相关
    private Connection connection = null;
    //上报信息统计缓存表
    private Table reportStatTable;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", parameterTool.getRequired("hbase.zookeeper.quorum"));
        configuration.set("zookeeper.znode.parent", parameterTool.getRequired("zookeeper.znode.parent"));
        connection = ConnectionFactory.createConnection(configuration);

        reportStatTable = connection.getTable(TableName.valueOf(parameterTool.getRequired("hbase.report.stat.table")));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (reportStatTable != null){
            reportStatTable.close();
        }
        if (connection != null){
            connection.close();
        }
    }

    @Override
    public void processElement(HourSumReportId hourSumReportId, Context context, Collector<HourSumReportId> collector) throws Exception {
        StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
        if(hourSumReportId.getAreaId() == null || hourSumReportId.getCollectDate() == null){
            return;
        }
        sj.add(hourSumReportId.getAreaId().toString());
        sj.add(hourSumReportId.getCollectDate());
        sj.add(String.valueOf(hourSumReportId.getUnitHour()));
        String key = sj.toString();
        byte[] rowKey = MD5Hash.getMD5AsHex((key).getBytes()).getBytes();

        //从hbase拉取数据
        Get get = new Get(rowKey);
        Result result = reportStatTable.get(get);
        Put hbasePut = new Put(rowKey);
        if (result == null) {
            hbasePut.addColumn(Constants.HBASE_FAMILY, "createTime".getBytes(), hourSumReportId.getCreateTime().getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "id".getBytes(), hourSumReportId.getId().toString().getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "collectPersons".getBytes(), String.valueOf(hourSumReportId.getCollectPersons()).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "collectSampleNum".getBytes(), String.valueOf(hourSumReportId.getCollectSampleNum()).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "transferPersons".getBytes(), String.valueOf(hourSumReportId.getTransferPersons()).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "transferSampleNum".getBytes(), String.valueOf(hourSumReportId.getTransferSampleNum()).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "receivePersons".getBytes(), String.valueOf(hourSumReportId.getReceivePersons()).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "receiveSampleNum".getBytes(), String.valueOf(hourSumReportId.getReceiveSampleNum()).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "checkPersons".getBytes(), String.valueOf(hourSumReportId.getCheckPersons()).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "checkSampleNum".getBytes(), String.valueOf(hourSumReportId.getCheckSampleNum()).getBytes());
        }else{
            String createTime = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "createTime".getBytes()));
            Long id = Bytes.toLong(result.getValue(Constants.HBASE_FAMILY, "id".getBytes()));
            int collectPersons = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "collectPersons".getBytes()))
                    + hourSumReportId.getCollectPersons();
            int collectSampleNum = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "collectSampleNum".getBytes()))
                    + hourSumReportId.getCollectSampleNum();
            int transferPersons = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "transferPersons".getBytes()))
                    + hourSumReportId.getTransferPersons();
            int transferSampleNum = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "transferSampleNum".getBytes()))
                    + hourSumReportId.getTransferSampleNum();
            int receivePersons = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "receivePersons".getBytes()))
                    + hourSumReportId.getReceivePersons();
            int receiveSampleNum = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "receiveSampleNum".getBytes()))
                    + hourSumReportId.getReceiveSampleNum();
            int checkPersons = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "checkPersons".getBytes()))
                    + hourSumReportId.getCheckPersons();
            int checkSampleNum = Bytes.toInt(result.getValue(Constants.HBASE_FAMILY, "checkSampleNum".getBytes()))
                    + hourSumReportId.getCheckSampleNum();
            hourSumReportId.setCreateTime(createTime);
            hourSumReportId.setId(id);
            hourSumReportId.setCollectPersons(collectPersons);
            hourSumReportId.setCollectSampleNum(collectSampleNum);
            hourSumReportId.setTransferPersons(transferPersons);
            hourSumReportId.setTransferSampleNum(transferSampleNum);
            hourSumReportId.setReceivePersons(receivePersons);
            hourSumReportId.setReceiveSampleNum(receiveSampleNum);
            hourSumReportId.setCheckPersons(checkPersons);
            hourSumReportId.setCheckSampleNum(checkSampleNum);

            hbasePut.addColumn(Constants.HBASE_FAMILY, "collectPersons".getBytes(), String.valueOf(collectPersons).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "collectSampleNum".getBytes(), String.valueOf(collectSampleNum).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "transferPersons".getBytes(), String.valueOf(transferPersons).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "transferSampleNum".getBytes(), String.valueOf(transferSampleNum).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "receivePersons".getBytes(), String.valueOf(receivePersons).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "receiveSampleNum".getBytes(), String.valueOf(receiveSampleNum).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "checkPersons".getBytes(), String.valueOf(checkPersons).getBytes());
            hbasePut.addColumn(Constants.HBASE_FAMILY, "checkSampleNum".getBytes(), String.valueOf(checkSampleNum).getBytes());
        }
        reportStatTable.put(hbasePut);
        collector.collect(hourSumReportId);
    }
}
