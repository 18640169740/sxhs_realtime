package com.sxhs.realtime.driver;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.sxhs.realtime.common.BaseJob;
import com.sxhs.realtime.common.Constants;
import com.sxhs.realtime.operator.ReportLogProcess;
import com.sxhs.realtime.operator.ReportStatProcess;
import com.sxhs.realtime.util.JobUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
 * @Description: 上报日志同步任务
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 18:17
 */
public class ReportLogDriver extends BaseJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9092,10.17.41.133:9092,10.17.41.134:9092",
                    "--group.id", "zjw_test1",
                    "--source.topic.name", "NUC_DATA_ZJW",
                    "--hbase.zookeeper.quorum", "10.17.41.132:2181,10.17.41.133:2181,10.17.41.134:2181",
                    "--zookeeper.znode.parent", "/dqhbase",
                    "--hbase.relation.table", "nuc_relation_distinct",
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/zhangjunwei_report_log_jar", true));
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
//        DataStreamSource<String> kafkaSource = env.readTextFile("C:\\Users\\q4189\\Desktop\\test.txt");

        //日志统计
        SingleOutputStreamOperator<String> dataStream = kafkaSource.process(new ReportLogProcess());

        DataStream<String> uploadLogStream = dataStream.getSideOutput(Constants.UPLOAD_LOG_TAG);
        DataStream<String> uploadLogFailStream = dataStream.getSideOutput(Constants.UPLOAD_LOG_FAIL_TAG);
        DataStream<String> uploadReportStream = dataStream.getSideOutput(Constants.UPLOAD_REPORT_TAG);
        DataStream<String> uploadReportFailStream = dataStream.getSideOutput(Constants.UPLOAD_REPORT_FAIL_TAG);

        SinkFunction<String> uploadLogSink = JobUtils.getStarrocksSink("upload_log");
        SinkFunction<String> uploadLogFailSink = JobUtils.getStarrocksSink("upload_log_fail");
        SinkFunction<String> uploadReportSink = JobUtils.getStarrocksSink("upload_report");
        SinkFunction<String> uploadReportFailSink = JobUtils.getStarrocksSink("upload_report_fail");

        //输出到starrocks
        uploadLogStream.addSink(uploadLogSink);
        uploadLogFailStream.addSink(uploadLogFailSink);
        uploadReportStream.addSink(uploadReportSink);
        uploadReportFailStream.addSink(uploadReportFailSink);

        env.execute("zhangjunwei_report_log_jar");
    }
}
