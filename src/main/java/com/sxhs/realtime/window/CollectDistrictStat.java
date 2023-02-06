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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// 采样点维度统计任务
public class CollectDistrictStat extends BaseJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9091,10.17.41.133:9091,10.17.41.134:9091",
                    "--group.id", "huquan_CollectDistrictStat",
                    "--source.topic.name", "NUC_DATA_ID"
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/huquan_CollectDistrictStat", true));
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

        Table unionTable = _unionStat(tEnv);
        tEnv.createTemporaryView("union_table", unionTable);

        // 注册check_org表
        tEnv.executeSql("CREATE TEMPORARY TABLE check_org ( " +
                "  id BIGINT, " +
                "  org_name string, " +
                "  PRIMARY KEY(id) NOT ENFORCED) " +
                "WITH ( " +
                "  'lookup.cache.max-rows' = '1000', " +
                "  'lookup.cache.ttl' = '24 hour', " +
                "  'connector' = 'jdbc', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'url' = 'jdbc:mysql://10.17.41.138:9030/nuc_db', " +
                "  'username' = 'huquan', " +
                "  'password' = 'oNa46nj0o65b@kvK', " +
                "  'table-name' = 'check_org' " +
                ")");

        // 关联org_id
        Table joinTable = tEnv.sqlQuery("select " +
                "t1.union_id union_id, " +
                "t1.area_id area_id, " +
                "t2.id org_code, " +
                "t1.org_name org_name, " +
                "t1.collect_location_name collect_location_name, " +
                "t1.source source, " +
                "t1.upload_number upload_number, " +
                "t1.success_number success_number, " +
                "t1.tube_number tube_number, " +
                "t1.upload_time upload_time, " +
                "t1.create_by create_by, " +
                "t1.collect_location_district collect_location_district " +
                "from union_table as t1 " +
                "left join check_org FOR SYSTEM_TIME AS OF t1.pt as t2 " +
                "on t1.org_name=t2.org_name");
        DataStream<Row> joinStream = tEnv.toAppendStream(joinTable, Row.class);

        SingleOutputStreamOperator<String> resultStream = joinStream.keyBy(row -> row.getField(0))
                .map(new RichMapFunction<Row, String>() {
                    private ValueState<JSONObject> cacheState;
                    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("CollectDistrictStat", JSONObject.class);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(48))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        cacheState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public String map(Row row) throws Exception {
                        JSONObject result = new JSONObject();
                        JSONObject cache = cacheState.value();
                        if (cache == null) {
                            cache = new JSONObject();
                            cache.put("id", SnowflakeIdWorker.generateId());
                            cache.put("create_time", sdf.format(new Date()));
                        }
                        result.put("id", cache.getString("id"));
                        result.put("area_id", row.getField(1));
                        result.put("org_code", row.getField(2));
                        result.put("org_name", row.getField(3));
                        result.put("collect_location_name", row.getField(4));
                        result.put("source", row.getField(5));
                        result.put("upload_number", (long) row.getField(6) + cache.getLongValue("upload_number"));
                        result.put("success_number", (long) row.getField(7) + cache.getLongValue("success_number"));
//                        result.put("fail_number", null);
                        result.put("tube_number", (long) row.getField(8) + cache.getLongValue("tube_number"));
                        result.put("upload_time", row.getField(9));
                        result.put("create_by", row.getField(10));
                        result.put("create_time", cache.getString("create_time"));
                        result.put("is_delete", 0);
                        result.put("collect_location_district", row.getField(11));
                        cacheState.update(result);
                        return result.toJSONString();
                    }
                });
        SinkFunction<String> srSink = JobUtils.getStarrocksSink("upload_log_org");
        resultStream.addSink(srSink);
        env.execute("huquan_CollectDistrictStat");
    }

    private static Table _unionStat(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(areaId as string),collectOrgName,collectLocationDistrict,collectLocationName,'1',substring(max(interfaceRecTime) from 0 for 10)) union_id, " +
                "areaId area_id, " +
                "collectOrgName org_name, " +
                "collectLocationName collect_location_name, " +
                "1 source, " +
                "count(1) upload_number, " +
                "count(distinct concat(collectTime,personIdCard)) success_number, " +
                "count(distinct tubeCode) tube_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "PROCTIME() as pt, " +
                "collectLocationDistrict collect_location_district " +
                "from collect_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId,collectOrgName,collectLocationDistrict,collectLocationName,'1' " +
                "union all " +
                // transport
                "select " +
                "concat_ws('_',cast(areaId as string),transportOrgName,'',collectAddress,'2',substring(max(interfaceRecTime) from 0 for 10)) union_id, " +
                "areaId area_id, " +
                "transportOrgName org_name, " +
                "collectAddress collect_location_name, " +
                "2 source, " +
                "count(1) upload_number, " +
                "0 success_number, " +
                "sum(tubeNum) tube_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "PROCTIME() as pt, " +
                "'' collect_location_district " +
                "from transport_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId,transportOrgName,'',collectAddress,'2' " +
                "union all " +
                // receive
                "select " +
                "concat_ws('_',cast(areaId as string),receiveOrgName,'','','3',substring(max(interfaceRecTime) from 0 for 10)) union_id, " +
                "areaId area_id, " +
                "receiveOrgName org_name, " +
                "'' collect_location_name, " +
                "3 source, " +
                "count(1) upload_number, " +
                "0 success_number, " +
                "sum(tubeNum) tube_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "PROCTIME() as pt, " +
                "'' collect_location_district " +
                "from receive_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId,receiveOrgName,'','','3' " +
                "union all " +
                // report
                "select " +
                "concat_ws('_',cast(areaId as string),checkOrgName,collectLocationDistrict,collectLocationName,'4',substring(max(interfaceRecTime) from 0 for 10)) union_id, " +
                "areaId area_id, " +
                "checkOrgName org_name, " +
                "collectLocationName collect_location_name, " +
                "4 source, " +
                "count(1) upload_number, " +
                "count(distinct concat(collectTime,personIdCard)) success_number, " +
                "count(distinct tubeCode) tube_number, " +
                "max(interfaceRecTime) upload_time, " +
                "max(userName) create_by, " +
                "PROCTIME() as pt, " +
                "collectLocationDistrict collect_location_district " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' MINUTE), " +
                "areaId,checkOrgName,collectLocationDistrict,collectLocationName,'4' " +
                "");
    }
}
