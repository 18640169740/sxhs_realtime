package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.common.BaseJob;
import com.sxhs.realtime.util.JobUtils;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import com.sxhs.realtime.util.StreamUtil;
import com.sxhs.realtime.util.TableUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// 数据上报延迟事件统计任务
public class ReportDelayStat extends BaseJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9091,10.17.41.133:9091,10.17.41.134:9091",
                    "--group.id", "huquan_ReportDelayStat",
                    "--source.topic.name", "NUC_DATA_ID"
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/huquan_ReportDelayStat", true));
        env.getConfig().isUseSnapshotCompression();

        //设置kafka参数
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterMap.get("bootstrap.servers"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, parameterMap.get("group.id"));
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>(parameterMap.get("source.topic.name"), new SimpleStringSchema(), properties);
        input.setStartFromGroupOffsets();

        //读取kafka数据
        DataStreamSource<String> sourceStream = env.addSource(input);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 侧输出流，将kafka数据分离，collect、transport、receive、report
        Tuple4<SingleOutputStreamOperator<CollectDataId>, DataStream<TransportDataId>, DataStream<ReceiveDataId>, DataStream<ReportDataId>> streams = StreamUtil.sideOutput(sourceStream);

        // stream转table
        Table collectDataTable = TableUtil.collectTable(tEnv, streams.f0);
        Table transportDataTable = TableUtil.transportTable(tEnv, streams.f1);
        Table receiveDataTable = TableUtil.receiveTable(tEnv, streams.f2);
        Table reportDataTable = TableUtil.reportTable(tEnv, streams.f3);

        // 建表
        tEnv.createTemporaryView("collect_data", collectDataTable);
        tEnv.createTemporaryView("transport_data", transportDataTable);
        tEnv.createTemporaryView("receive_data", receiveDataTable);
        tEnv.createTemporaryView("report_data", reportDataTable);

        Table collectStat = _statCollect(tEnv);
        Table transportStat = _statTransport(tEnv);
        Table receiveState = _statReceive(tEnv);
        Table reportState = _statReport(tEnv);

        Table unionTable = collectStat.unionAll(transportStat).unionAll(receiveState).unionAll(reportState);
        tEnv.createTemporaryView("union_table", unionTable);

        // 注册client_data表
        tEnv.executeSql("CREATE TEMPORARY TABLE client_data ( " +
                "  client_id BIGINT, " +
                "  client_name string, " +
                "  PRIMARY KEY(client_id) NOT ENFORCED) " +
                "WITH ( " +
                "  'lookup.cache.max-rows' = '1000', " +
                "  'lookup.cache.ttl' = '24 hour', " +
                "  'connector' = 'jdbc', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'url' = 'jdbc:mysql://10.17.41.138:9030/nuc_db', " +
                "  'username' = 'huquan', " +
                "  'password' = 'oNa46nj0o65b@kvK', " +
                "  'table-name' = 'client_data' " +
                ")");
        // 关联client_data
        Table joinTable = tEnv.sqlQuery("select " +
                "t1.union_id, " +
                "t1.area_id, " +
                "t1.source, " +
                "t1.data_delay_time, " +
                "t1.delay_sum, " +
                "t1.delay_count, " +
                "if(t2.client_name=null,t2.client_name,'') report_client, " +
                "t1.create_time create_time " +
                "from union_table as t1 " +
                "left join client_data FOR SYSTEM_TIME AS OF t1.pt as t2 " +
                "on t1.client_id=t2.client_id");

        DataStream<Row> joinStream = tEnv.toAppendStream(joinTable, Row.class);
        SingleOutputStreamOperator<String> resultStream = joinStream.keyBy(row -> row.getField(0))
                .map(new RichMapFunction<Row, String>() {
                    private ValueState<JSONObject> cacheState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("ReportDelayStat", JSONObject.class);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(48))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        cacheState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public String map(Row row) throws Exception {
                        JSONObject jsonObj = new JSONObject();
                        jsonObj.put("area_id", row.getField(1));
                        jsonObj.put("source", row.getField(2));
                        jsonObj.put("data_delay_time", row.getField(3));
                        jsonObj.put("report_client", row.getField(6));
                        JSONObject cache = cacheState.value();
                        if (cache == null) {
                            cache = new JSONObject();
                        }
                        jsonObj.put("create_time", cache.get("create_time") == null ? row.getField(7) : cache.get("create_time"));
                        jsonObj.put("id", cache.getLong("id") == null ? SnowflakeIdWorker.generateId() : cache.getLong("id"));
                        int delaySum = (int) row.getField(4) + cache.getIntValue("delay_sum");
                        long delayCount = (long) row.getField(5) + cache.getLongValue("delayCount");

                        // 更新cache
                        cache = JSONObject.parseObject(jsonObj.toJSONString());
                        cache.put("delay_sum", delaySum);
                        cache.put("delay_count", delayCount);
                        cacheState.update(cache);
                        // 计算平均值
                        jsonObj.put("delay_num", delaySum / delayCount);
                        jsonObj.put("delay_type", "上报时间延迟");
                        return jsonObj.toJSONString();
                    }
                });
        SinkFunction<String> srSink = JobUtils.getStarrocksSink("data_time_delay");

        resultStream.addSink(srSink);

        env.execute("huquan_ReportDelayStat");
    }

    private static Table _statReport(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(t1.area_id as string),'4',cast(t1.client_id as string),t1.data_delay_time) union_id, " +
                "t1.area_id area_id, " +
                "'4' source, " +
                "t1.data_delay_time, " +
                "t1.delay_sum, " +
                "t1.delay_count, " +
                "t1.client_id client_id, " +
                "PROCTIME() as pt, " +
                "t1.create_time create_time " +
                "from " +
                "(select " +
                "areaId area_id, " +
                "concat(DATE_FORMAT(TIMESTAMPADD(HOUR,8,pt),'yyyy-MM-dd HH时'),DATE_FORMAT(TIMESTAMPADD(HOUR,9,pt),'-HH时')) data_delay_time, " +
                "sum(timestampdiff(minute,to_timestamp(checkTime),LOCALTIMESTAMP)) delay_sum, " +
                "count(1) delay_count, " +
                "clientId client_id, " +
                "max(interfaceRecTime) create_time " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId,clientId, " +
                "concat(DATE_FORMAT(TIMESTAMPADD(HOUR,8,pt),'yyyy-MM-dd HH时'),DATE_FORMAT(TIMESTAMPADD(HOUR,9,pt),'-HH时'))" +
                ") as t1");
    }

    private static Table _statReceive(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(t1.area_id as string),'3',cast(t1.client_id as string),t1.data_delay_time) union_id, " +
                "t1.area_id area_id, " +
                "'3' source, " +
                "t1.data_delay_time, " +
                "t1.delay_sum, " +
                "t1.delay_count, " +
                "t1.client_id client_id, " +
                "PROCTIME() as pt, " +
                "t1.create_time create_time " +
                "from " +
                "(select " +
                "areaId area_id, " +
                "concat(DATE_FORMAT(TIMESTAMPADD(HOUR,8,pt),'yyyy-MM-dd HH时'),DATE_FORMAT(TIMESTAMPADD(HOUR,9,pt),'-HH时')) data_delay_time, " +
                "sum(timestampdiff(minute,to_timestamp(receiveTime),LOCALTIMESTAMP)) delay_sum, " +
                "count(1) delay_count, " +
                "clientId client_id, " +
                "max(interfaceRecTime) create_time " +
                "from receive_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId,clientId, " +
                "concat(DATE_FORMAT(TIMESTAMPADD(HOUR,8,pt),'yyyy-MM-dd HH时'),DATE_FORMAT(TIMESTAMPADD(HOUR,9,pt),'-HH时'))" +
                ") as t1");
    }

    private static Table _statTransport(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(t1.area_id as string),'2',cast(t1.client_id as string),t1.data_delay_time) union_id, " +
                "t1.area_id area_id, " +
                "'2' source, " +
                "t1.data_delay_time, " +
                "t1.delay_sum, " +
                "t1.delay_count, " +
                "t1.client_id client_id, " +
                "PROCTIME() as pt, " +
                "t1.create_time create_time " +
                "from " +
                "(select " +
                "areaId area_id, " +
                "concat(DATE_FORMAT(TIMESTAMPADD(HOUR,8,pt),'yyyy-MM-dd HH时'),DATE_FORMAT(TIMESTAMPADD(HOUR,9,pt),'-HH时')) data_delay_time, " +
                "sum(timestampdiff(minute,to_timestamp(deliveryTime),LOCALTIMESTAMP)) delay_sum, " +
                "count(1) delay_count, " +
                "clientId client_id, " +
                "max(interfaceRecTime) create_time " +
                "from transport_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId,clientId, " +
                "concat(DATE_FORMAT(TIMESTAMPADD(HOUR,8,pt),'yyyy-MM-dd HH时'),DATE_FORMAT(TIMESTAMPADD(HOUR,9,pt),'-HH时'))" +
                ") as t1");
    }

    private static Table _statCollect(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(t1.area_id as string),'1',cast(t1.client_id as string),t1.data_delay_time) union_id, " +
                "t1.area_id area_id, " +
                "'1' source, " +
                "t1.data_delay_time, " +
                "t1.delay_sum, " +
                "t1.delay_count, " +
                "t1.client_id client_id, " +
                "PROCTIME() as pt, " +
                "t1.create_time create_time " +
                "from " +
                "(select " +
                "areaId area_id, " +
                "concat(DATE_FORMAT(TIMESTAMPADD(HOUR,8,pt),'yyyy-MM-dd HH时'),DATE_FORMAT(TIMESTAMPADD(HOUR,9,pt),'-HH时')) data_delay_time, " +
                // 保存延迟sum与count值，用于计算avg
                "sum(timestampdiff(minute,to_timestamp(collectTime),LOCALTIMESTAMP)) delay_sum, " +
                "count(1) delay_count, " +
                "clientId client_id, " +
                "max(interfaceRecTime) create_time " +
                "from collect_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId,clientId, " +
                "concat(DATE_FORMAT(TIMESTAMPADD(HOUR,8,pt),'yyyy-MM-dd HH时'),DATE_FORMAT(TIMESTAMPADD(HOUR,9,pt),'-HH时'))" +
                ") as t1");
    }
}
