package com.sxhs.realtime.driver;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.HourSumReportId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.common.BaseJob;
import com.sxhs.realtime.operator.JsonToHourSumReport;
import com.sxhs.realtime.operator.ReportStatProcess;
import com.sxhs.realtime.util.JobUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Description: 上报统计信息同步任务
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 16:33
 */
public class ReportStatDriver extends BaseJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9092,10.17.41.133:9092,10.17.41.134:9092",
                    "--group.id", "zjw_test1",
                    "--source.topic.name", "NUC_DATA_ZJW",
                    "--hbase.zookeeper.quorum", "10.17.41.132:2181,10.17.41.133:2181,10.17.41.134:2181",
                    "--zookeeper.znode.parent", "/dqhbase",
                    "--hbase.report.stat.table", "nuc_report_stat",
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/zhangjunwei_report_stat_jar", true));
        env.getConfig().isUseSnapshotCompression();

        //设置kafka参数
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterMap.get("bootstrap.servers"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, parameterMap.get("group.id"));
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>(parameterMap.get("source.topic.name"), new SimpleStringSchema(), properties);
        input.setStartFromGroupOffsets();
//        input.setStartFromEarliest();
        //读取kafka数据
        DataStreamSource<String> kafkaSource = env.addSource(input);

        //数据格式转换
        SingleOutputStreamOperator<HourSumReportId> dataStream = kafkaSource.flatMap(new JsonToHourSumReport());

        //数据统计
        DataStream<HourSumReportId> hourSumReportIdDataStream = dataStream.keyBy(new KeySelector<HourSumReportId, Tuple3<String,String,String>>() {
            @Override
            public Tuple3<String, String, String> getKey(HourSumReportId hourSumReportId) throws Exception {
                return new Tuple3<>(hourSumReportId.getAreaId().toString(), hourSumReportId.getCollectDate(), String.valueOf(hourSumReportId.getUnitHour()));
            }
        }).process(new ReportStatProcess()).rebalance();

        //格式转换
        DataStream<String> strStream = hourSumReportIdDataStream.map(new MapFunction<HourSumReportId, String>() {
            @Override
            public String map(HourSumReportId hourSumReportId) throws Exception {
                JSONObject jsonObj = new JSONObject();
                jsonObj.put("create_time", hourSumReportId.getCreateTime());
                jsonObj.put("area_id", hourSumReportId.getAreaId());
                jsonObj.put("id", hourSumReportId.getId());
                jsonObj.put("collect_date", hourSumReportId.getCollectDate());
                jsonObj.put("unit_hour", hourSumReportId.getUnitHour());
                jsonObj.put("collect_persons", hourSumReportId.getCollectPersons());
                jsonObj.put("collect_sample_num", hourSumReportId.getCheckSampleNum());
                jsonObj.put("transfer_persons", hourSumReportId.getTransferPersons());
                jsonObj.put("transfer_sample_num", hourSumReportId.getTransferSampleNum());
                jsonObj.put("receive_persons", hourSumReportId.getReceivePersons());
                jsonObj.put("receive_sample_num", hourSumReportId.getReceiveSampleNum());
                jsonObj.put("check_persons", hourSumReportId.getCheckPersons());
                jsonObj.put("check_sample_num", hourSumReportId.getCheckSampleNum());
                jsonObj.put("update_time", hourSumReportId.getUpdateTime());
                jsonObj.put("is_delete", hourSumReportId.getIsDelete());
                jsonObj.put("create_by", hourSumReportId.getCreateBy());
                return jsonObj.toJSONString();
            }
        });

        SinkFunction<String> sink = JobUtils.getStarrocksSink("hour_sum_report");

        strStream.addSink(sink);

        env.execute("zhangjunwei_report_stat_jar");

    }
}
