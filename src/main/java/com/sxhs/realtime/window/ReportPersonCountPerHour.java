package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.util.StreamUtil;
import com.sxhs.realtime.util.TableUtil;
import com.sxhs.realtime.util.UploadNumberUdf;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

// 小时维度上报人次统计任务
public class ReportPersonCountPerHour {
    private static final Logger logger = LoggerFactory.getLogger(ReportPersonCountPerHour.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 7777);

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

        tEnv.createTemporarySystemFunction("check_persons", UploadNumberUdf.class);

        Table collectStat = _statCollect(tEnv);
        Table reportState = _statReport(tEnv);

        Table unionTable = collectStat.unionAll(reportState);
        DataStream<Row> unionStream = tEnv.toAppendStream(unionTable, Row.class);

        SingleOutputStreamOperator<String> resultStream = unionStream.keyBy(row -> row.getField(0)).map(new RichMapFunction<Row, String>() {
            private ValueState<JSONObject> cacheState;
            private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("CollectAndReportTimeDifStat", JSONObject.class);
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
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
                    // TODO 生成id
                    cache.put("id", 123456l);
                    cache.put("create_time", sdf.format(new Date()));
                }
                result.put("id", cache.getLongValue("id"));
                result.put("area_id", row.getField(1));
                result.put("unit_hour", row.getField(2));
                result.put("collect_persons", (long) row.getField(3) + cache.getLongValue("collect_persons"));
                result.put("transfer_persons", 0);
                result.put("receive_persons", 0);
                int checkPersons = (int) row.getField(4) + cache.getIntValue("check_persons");
                result.put("check_persons", checkPersons);
                result.put("create_time", cache.getString("create_time"));
                result.put("is_delete", 0);
                result.put("check_numbers", checkPersons);
                result.put("collect_all", (long) row.getField(5) + cache.getLongValue("collect_all"));
                result.put("check_all", (long) row.getField(6) + cache.getLongValue("check_all"));
                cacheState.update(result);
                return result.toJSONString();
            }
        });

        // TODO 入库
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Table _statReport(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(areaId as string),cast(hour(TO_TIMESTAMP(collectTime)) as string)) union_id, " +
                "areaId area_id, " +
                "hour(TO_TIMESTAMP(collectTime)) unit_hour, " +
                "cast(0 as bigint) collect_persons, " +
                "check_persons(distinct concat_ws('_',personIdCard,tubeCode)) check_persons, " +
                "0 collect_all, " +
                "count(1) check_all " +
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId, " +
                "hour(TO_TIMESTAMP(collectTime))");
    }

    private static Table _statCollect(StreamTableEnvironment tEnv) {
        return tEnv.sqlQuery("select " +
                "concat_ws('_',cast(areaId as string),cast(hour(TO_TIMESTAMP(collectTime)) as string)) union_id, " +
                "areaId area_id, " +
                "hour(TO_TIMESTAMP(collectTime)) unit_hour, " +
                "count(distinct personIdCard) collect_persons, " +
                "0 check_persons, " +
                "count(1) collect_all, " +
                "0 check_all " +
                "from collect_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId, " +
                "hour(TO_TIMESTAMP(collectTime))");
    }
}
