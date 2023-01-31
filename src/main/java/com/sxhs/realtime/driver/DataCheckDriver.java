package com.sxhs.realtime.driver;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.sxhs.realtime.bean.*;
import com.sxhs.realtime.common.BaseJob;
import com.sxhs.realtime.common.Constants;
import com.sxhs.realtime.operator.DataCheckProcess;
import com.sxhs.realtime.operator.ProblemDataStat;
import com.sxhs.realtime.util.JobUtils;
import com.sxhs.realtime.util.StreamUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Description: 数据校验
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/29 11:20
 */
public class DataCheckDriver extends BaseJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9092,10.17.41.133:9092,10.17.41.134:9092",
                    "--group.id", "zjw_test1",
                    "--source.topic.name", "NUC_DATA_ZJW",
                    "--hbase.zookeeper.quorum", "10.17.41.132:2181,10.17.41.133:2181,10.17.41.134:2181",
                    "--zookeeper.znode.parent", "/dqhbase",
                    "--hbase.relation.table", "nuc_relation_distinct",
                    "--hbase.check.table", "nuc_check_distinct",
                    "--hbase.upload.log.table", "nuc_upload_log_stat",
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/zhangjunwei_data_check_jar", true));
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

        // 侧输出流，将kafka数据分离，collect、transport、receive、report
        Tuple4<SingleOutputStreamOperator<CollectDataId>, DataStream<TransportDataId>, DataStream<ReceiveDataId>, DataStream<ReportDataId>> streams = StreamUtil.sideOutput(kafkaSource);

        SingleOutputStreamOperator<CollectDataId> collectStream = streams.f0;
        DataStream<TransportDataId> transportStream = streams.f1;
        DataStream<ReceiveDataId> receiveStream = streams.f2;
        DataStream<ReportDataId> reportStream = streams.f3;
        //各环节进行二次校验
        SingleOutputStreamOperator<Object> collectCheckStream = collectStream.process(new DataCheckProcess<>());
        DataStream<ProblemDataCr> collectCrStream = collectCheckStream.getSideOutput(Constants.CR_TAG);
        DataStream<ProblemDataTre> collectTreStream = collectCheckStream.getSideOutput(Constants.TRE_TAG);
        SingleOutputStreamOperator<Object> transportCheckStream = transportStream.process(new DataCheckProcess<>());
        DataStream<ProblemDataCr> tranportCrStream = transportCheckStream.getSideOutput(Constants.CR_TAG);
        DataStream<ProblemDataTre> tranportTreStream = transportCheckStream.getSideOutput(Constants.TRE_TAG);
        SingleOutputStreamOperator<Object> receiveCheckStream = receiveStream.process(new DataCheckProcess<>());
        DataStream<ProblemDataCr> receiveCrStream = receiveCheckStream.getSideOutput(Constants.CR_TAG);
        DataStream<ProblemDataTre> receiveTreStream = receiveCheckStream.getSideOutput(Constants.TRE_TAG);
        SingleOutputStreamOperator<Object> reportCheckStream = reportStream.process(new DataCheckProcess<>());
        DataStream<ProblemDataCr> reportCrStream = reportCheckStream.getSideOutput(Constants.CR_TAG);
        DataStream<ProblemDataTre> reportTreStream = reportCheckStream.getSideOutput(Constants.TRE_TAG);
        //工单流
        DataStream<ProblemDataCr> crDataStream = collectCrStream.union(tranportCrStream).union(receiveCrStream).union(reportCrStream);
        DataStream<ProblemDataTre> treDataStream = collectTreStream.union(tranportTreStream).union(receiveTreStream).union(reportTreStream);

        DataStream<String> crStrStream= crDataStream.map(new MapFunction<ProblemDataCr, String>() {
            @Override
            public String map(ProblemDataCr problemDataCr) throws Exception {
                return JSONObject.toJSONString(problemDataCr);
            }
        });
        DataStream<String> treStrStream= treDataStream.map(new MapFunction<ProblemDataTre, String>() {
            @Override
            public String map(ProblemDataTre problemDataTre) throws Exception {
                return JSONObject.toJSONString(problemDataTre);
            }
        });

        //工单流合并
        DataStream<ProblemData> problemStream = crDataStream.map(new MapFunction<ProblemDataCr, ProblemData>() {
            @Override
            public ProblemData map(ProblemDataCr problemDataCr) throws Exception {
                ProblemData problemData = problemDataCr;
                return problemData;
            }
        }).union(treDataStream.map(new MapFunction<ProblemDataTre, ProblemData>() {
            @Override
            public ProblemData map(ProblemDataTre problemDataTre) throws Exception {
                ProblemData problemData = problemDataTre;
                return problemData;
            }
        }));

        //工单统计
        DataStream<String> problemStatStream = problemStream.keyBy(new KeySelector<ProblemData, Tuple3<String,String,String>>() {
            @Override
            public Tuple3<String, String, String> getKey(ProblemData problemData) throws Exception {
                String area_id = problemData.getArea_id().toString();
                String source = problemData.getSource().toString();
                String createTime = problemData.getCreate_time().substring(0, 10);
                return new Tuple3<>(area_id, source, createTime);
            }
        }).process(new ProblemDataStat());

        //输出工单统计数据
        SinkFunction<String> sink = JobUtils.getStarrocksSink("upload_log_prob");
        problemStatStream.addSink(sink);

        //输出工单数据
        SinkFunction<String> crSink = JobUtils.getStarrocksSink("problem_data_cr");
        SinkFunction<String> treSink = JobUtils.getStarrocksSink("problem_data_tre");

        crStrStream.addSink(crSink);
        treStrStream.addSink(treSink);

        env.execute("zhangjunwei_data_check_jar");

    }
}
