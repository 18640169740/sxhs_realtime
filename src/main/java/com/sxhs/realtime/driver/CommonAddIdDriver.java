package com.sxhs.realtime.driver;

import com.sxhs.realtime.bean.CommonDuplicateData;
import com.sxhs.realtime.common.BaseJob;
import com.sxhs.realtime.operator.CommonDuplicateDataToJson;
import com.sxhs.realtime.operator.FlinkVariablePartitioner;
import com.sxhs.realtime.operator.IdMappingByHbase;
import com.sxhs.realtime.operator.JsonToCommonDuplicateData;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * @Description: 生成唯一性ID，相同数据ID相同
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/13 14:57
 */
public class CommonAddIdDriver extends BaseJob {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9092,10.17.41.133:9092,10.17.41.134:9092",
                    "--group.id", "zjw_test1",
                    "--source.topic.name", "test1",
                    "--sink.topic.name", "zjw_test",
                    "--hbase.zookeeper.quorum", "10.17.41.132:2181,10.17.41.133:2181,10.17.41.134:2181",
                    "--zookeeper.znode.parent", "/dqhbase",
                    "--hbase.id.table", "nuc_id_distinct",
                    "--hbase.relation.table", "nuc_relation_distinct",
            };
        }

        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/zhangjunwei_common_id_mapping_jar", true));
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

        //数据解析
        DataStream<CommonDuplicateData> dataStream = kafkaSource.flatMap(new JsonToCommonDuplicateData()).name("dataPrase");

        //数据id去重
        dataStream = dataStream.keyBy(CommonDuplicateData::getType)
                .process(new IdMappingByHbase()).name("idMapping")
                .rebalance();

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

}
