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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
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

// 3.3.3.2.5各环节延迟统计任务
public class DelayStat extends BaseJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{
                    "--bootstrap.servers", "10.17.41.132:9091,10.17.41.133:9091,10.17.41.134:9091",
                    "--group.id", "huquan_DelayStat",
                    "--source.topic.name", "NUC_DATA_ID"
            };
        }
        //参数解析
        final ParameterTool parameterToolold = ParameterTool.fromArgs(args);
        Map<String, String> parameterMap = new HashMap<>(parameterToolold.toMap());
        //设置全局参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(parameterMap));
        //状态后端采用RocksDB/增量快照
        env.setStateBackend(new RocksDBStateBackend("hdfs://NSBD/warehouse_bigdata/realtimecompute/rtcalc/huquan_DelayStat", true));
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

        Table unionTable = collectStat
                .unionAll(transportStat)
                .unionAll(receiveState)
                .unionAll(reportState);
        DataStream<Row> unionStream = tEnv.toAppendStream(unionTable, Row.class);
        SingleOutputStreamOperator<String> resultStream = unionStream.map((MapFunction<Row, String>) row -> {
            JSONObject jsonObj = new JSONObject();
            jsonObj.put("id", SnowflakeIdWorker.generateId());
            jsonObj.put("area_id", row.getField(0));
            jsonObj.put("source", row.getField(1));
            jsonObj.put("day_date", row.getField(2));
            jsonObj.put("unit_hour", row.getField(3));
            jsonObj.put("delay_avg", row.getField(4));
            jsonObj.put("create_time", row.getField(5));
            jsonObj.put("unit_minute", row.getField(6));
            return jsonObj.toJSONString();
        });
        SinkFunction<String> srSink = JobUtils.getStarrocksSink("delay_avg");
        resultStream.addSink(srSink);
        env.execute("huquan_DelayStat");

    }

    private static Table _statReport(StreamTableEnvironment tEnv) {
        String sql = "select " +
                "areaId area_id," +
                "4 source, " +
                "SUBSTRING(max(checkTime) from 1 FOR 10) day_date, " +
                "HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' MINUTE))) unit_hour, " +
                "avg(timestampdiff(minute, TO_TIMESTAMP(checkTime), TO_TIMESTAMP(interfaceRecTime))) delay_avg, " +
                "max(interfaceRecTime) create_time, " +
                "MINUTE(TUMBLE_START(pt,INTERVAL '10' MINUTE)) unit_minute " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '10' MINUTE), " +
                // 按处理时间开床10分钟，窗口内数据的unit_hour，unit_minute相同，无需再分组
                "areaId," +
                "substring(interfaceRecTime from 0 for 9)";
        return tEnv.sqlQuery(sql);
    }

    private static Table _statReceive(StreamTableEnvironment tEnv) {
        String sql = "select " +
                "areaId area_id," +
                "3 source, " +
                "SUBSTRING(max(receiveTime) from 1 FOR 10) day_date, " +
                "HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' MINUTE))) unit_hour, " +
                "avg(timestampdiff(minute, TO_TIMESTAMP(receiveTime), TO_TIMESTAMP(interfaceRecTime))) delay_avg, " +
                "max(interfaceRecTime) create_time, " +
                "MINUTE(TUMBLE_START(pt,INTERVAL '10' MINUTE)) unit_minute " +
                "from receive_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '10' MINUTE), " +
                // 按处理时间开床10分钟，窗口内数据的unit_hour，unit_minute相同，无需再分组
                "areaId," +
                "substring(interfaceRecTime from 0 for 9)";
        return tEnv.sqlQuery(sql);
    }

    private static Table _statTransport(StreamTableEnvironment tEnv) {
        String sql = "select " +
                "areaId area_id," +
                "2 source, " +
                "SUBSTRING(max(deliveryTime) from 1 FOR 10) day_date, " +
                "HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' MINUTE))) unit_hour, " +
                "avg(timestampdiff(minute, TO_TIMESTAMP(deliveryTime), TO_TIMESTAMP(interfaceRecTime))) delay_avg, " +
                "max(interfaceRecTime) create_time, " +
                "MINUTE(TUMBLE_START(pt,INTERVAL '10' MINUTE)) unit_minute " +
                "from transport_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '10' MINUTE), " +
                // 按处理时间开床10分钟，窗口内数据的unit_hour，unit_minute相同，无需再分组
                "areaId," +
                "substring(interfaceRecTime from 0 for 9)";
        return tEnv.sqlQuery(sql);
    }

    private static Table _statCollect(StreamTableEnvironment tEnv) {
        String sql = "select " +
                "areaId area_id," +
                "1 source, " +
                "SUBSTRING(max(collectTime) from 1 FOR 10) day_date, " +
                "HOUR(TIMESTAMPADD(HOUR, 8,TUMBLE_START(pt,INTERVAL '10' MINUTE))) unit_hour, " +
                "avg(timestampdiff(minute, TO_TIMESTAMP(collectTime), TO_TIMESTAMP(interfaceRecTime))) delay_avg, " +
                "max(interfaceRecTime) create_time, " +
                "MINUTE(TUMBLE_START(pt,INTERVAL '10' MINUTE)) unit_minute " +
                "from collect_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '10' MINUTE), " +
                // 按处理时间开窗10分钟，窗口内数据的unit_hour，unit_minute相同，无需再分组
                "areaId," +
                "substring(interfaceRecTime from 0 for 9)";
        return tEnv.sqlQuery(sql);
    }
}
