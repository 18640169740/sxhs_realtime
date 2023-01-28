package com.sxhs.realtime.driver;

import com.sxhs.realtime.bean.CommonDuplicateData;
import com.sxhs.realtime.operator.CommonDuplicateDataToJson;
import com.sxhs.realtime.operator.FlinkVariablePartitioner;
import com.sxhs.realtime.operator.JsonToCommonDuplicateData;
import com.sxhs.realtime.operator.IdMappingByHbase;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @Description: 生成唯一性ID，相同数据ID相同
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/13 14:57
 */
public class CommonAddIdDriver {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9092,10.17.41.133:9092,10.17.41.134:9092",
                    "--group.id", "zjw_test1",
                    "--source.topic.name", "NUC_DATA_ZJW",
                    "--sink.topic.name", "NUC_DATA_ID_ZJW",
                    "--hbase.zookeeper.quorum", "10.17.41.132:2181,10.17.41.133:2181,10.17.41.134:2181",
                    "--zookeeper.znode.parent", "/dqhbase",
                    "--hbase.id.table", "nuc_id_distinct",
                    "--hbase.collect.table", "nuc_collect_distinct",
            };
        }

        //获取流环境
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));

        //设置kafka参数
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterMap.get("bootstrap.servers"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, parameterMap.get("group.id"));
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>(parameterMap.get("source.topic.name"), new SimpleStringSchema(), properties);
//        input.setStartFromGroupOffsets();
        input.setStartFromEarliest();
        //读取kafka数据
        DataStreamSource<String> kafkaSource = env.addSource(input);

        //数据解析
        DataStream<CommonDuplicateData> dataStream = kafkaSource.flatMap(new JsonToCommonDuplicateData()).name("dataPrase");

        //数据id去重
        dataStream = dataStream.keyBy(CommonDuplicateData::getType)
                .process(new IdMappingByHbase()).name("idMapping")
                .rescale();

        //将bean对象转成json字符串
        DataStream<String> dataStrStream = dataStream.map(new CommonDuplicateDataToJson()).name("toJson");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                parameterMap.get("sink.topic.name"),
                new SimpleStringSchema(),
                properties,
                (Optional)Optional.of(new FlinkVariablePartitioner()));

        dataStrStream.addSink(producer);

        env.execute("zhangjunwei_common_id_mapping_jar");

    }

    /**
     * 设置流执行环境
     *
     * @return
     * @throws IOException
     */
    private static StreamExecutionEnvironment getStreamExecutionEnvironment() throws IOException {
        //获取flink流式处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //重启机制，失败率重启机制，3分钟内重试三次，每过10秒重试一次
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(3, TimeUnit.MINUTES),
                Time.of(10, SECONDS)));
        //默认10分钟进行一次checkPoint
        env.enableCheckpointing(1 * 60 * 1000);
        //获取checkpoint配置
        CheckpointConfig config = env.getCheckpointConfig();
        //精准一次处理
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //配置checkpoint并行度，同一时间值允许进行一次检查点
        config.setMaxConcurrentCheckpoints(1);
        //默认配置checkpoint必须在5分钟内完成一次checkpoint，否则检查点终止
        config.setCheckpointTimeout(5 * 60 * 1000);
        //默认设置checkpoint最小时间间隔
        config.setMinPauseBetweenCheckpoints(5000);
        //在cancel任务时候，系统保留checkpoint
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //状态后端采用RocksDB/增量快照
//        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/common_addid_driver_02", true));
//        env.getConfig().isUseSnapshotCompression();
        //并行度
        env.setParallelism(1);
        return env;
    }
}
