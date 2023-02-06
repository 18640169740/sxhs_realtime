package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import com.sxhs.realtime.util.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

// 城市街道数据量统计任务
public class CityDistrictDataStat {
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

        // 注册临时方法
        tEnv.createTemporarySystemFunction("upload_number", UploadNumberUdf.class);
        tEnv.createTemporarySystemFunction("collect_check", CollectCheckUdf.class);
        tEnv.createTemporarySystemFunction("report_check", ReportCheckUdf.class);

        // 对collect、transport、receive、report数据分别做开窗统计
        Table collectStat = _statCollect(tEnv);
        // TODO transport和receive暂未做二次校验统计
        Table transportStat = _statTransport(tEnv);
        Table receiveState = _statReceive(tEnv);
        Table reportState = _statReport(tEnv);

        // 将统计后的表做union
        Table unionTable = collectStat.unionAll(transportStat).unionAll(receiveState).unionAll(reportState);

        DataStream<Row> unionStream = tEnv.toAppendStream(unionTable, Row.class);
        SingleOutputStreamOperator<String> resultStream = unionStream.keyBy(t -> t.getField(0))
                .flatMap(new RichFlatMapFunction<Row, String>() {
                    private MapState<Integer, JSONObject> cacheState;
                    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer, JSONObject> valueStateDescriptor = new MapStateDescriptor<>("CityDistrictDataStat", Integer.class, JSONObject.class);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(48))
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        valueStateDescriptor.enableTimeToLive(ttlConfig);
                        cacheState = getRuntimeContext().getMapState(valueStateDescriptor);
                    }

                    @Override
                    public void flatMap(Row row, Collector<String> collector) throws Exception {
                        int source = (int) row.getField(4);
                        JSONObject result = new JSONObject();
                        JSONObject cache = cacheState.get(source);
                        if (cache == null) {
                            result.put("id", SnowflakeIdWorker.generateId());
                            result.put("create_time", sdf.format(new Date()));
                            cache = new JSONObject();
                        } else {
                            result.put("id", cache.getLongValue("id"));
                            result.put("create_time", cache.getString("create_time"));
                        }

                        // 装载累计字段
                        // 默认赋值缓存，累计在report更新`
                        result.put("upload_number", cache.getIntValue("upload_number"));
                        result.put("success_number", (long) row.getField(6) + cache.getLongValue("success_number"));
                        result.put("tube_number", (long) row.getField(8) + cache.getLongValue("tube_number"));

                        // 二次检验
                        String problem = (String) row.getField(9);
                        int problemNumber = 0;
                        int problemError = 0;
                        int problemUnsync = 0;
                        if (problem != null) {
                            String[] s = problem.split(",", -1);
                            if (s != null && s.length == 3) {
                                problemNumber = Integer.valueOf(s[0]);
                                problemError = Integer.valueOf(s[1]);
                                problemUnsync = Integer.valueOf(s[2]);
                            }
                        }
                        result.put("problem_number", problemNumber + cache.getIntValue("problem_number"));
                        result.put("problem_error", problemError + cache.getIntValue("problem_error"));
                        result.put("problem_unsync", problemUnsync + cache.getIntValue("problem_unsync"));

                        result.put("problem_number_report", (long) row.getField(10) + cache.getLongValue("problem_number_report"));
                        result.put("problem_error", (int) row.getField(11) + cache.getIntValue("problem_error"));
                        result.put("problem_unsync", (int) row.getField(12) + cache.getIntValue("problem_unsync"));

                        // 装载非累计字段
                        result.put("area_id", row.getField(1));
                        result.put("collect_location_city", String.valueOf(row.getField(2)));
                        result.put("collect_location_district", String.valueOf(row.getField(3)));
                        result.put("source", source);
                        result.put("problem_repeat", 0);
                        result.put("fix_number", 0);
                        result.put("fix_number_report", 0);
                        result.put("upload_time", String.valueOf(row.getField(13)));
                        result.put("upload_result", 1);
                        result.put("create_by", String.valueOf(row.getField(14)));
                        result.put("is_delete", 0);
//                        result.put("fail_number", 0);

                        /**
                         * 当source=4时:
                         * 1.获取upload_time中的personIdCard_tubeCode信息
                         * 2.查询hbase，计算出采检一致指标值
                         * 3.更新相同unionid下的source=1的采检一致指标，并输出
                         * 4.更新缓存
                         */
                        if (source == 4) {
                            JSONObject collectCache = cacheState.get(1);
                            if (collectCache != null) {
                                int uploadNumber = (int) row.getField(5);
                                collectCache.put("upload_number", uploadNumber + collectCache.getIntValue("upload_number"));
                                collector.collect(collectCache.toJSONString());
                                cacheState.put(1, collectCache);
                            }
                        }
                        collector.collect(result.toJSONString());
                        // 更新缓存
//                        cache = JSONObject.parseObject(result.toJSONString());
                        cacheState.put(source, result);
                    }
                });
        SinkFunction<String> srSink = StarRocksSink.sink(
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", "jdbc:mysql://10.17.41.138:9030?nuc_db")
                        .withProperty("load-url", "10.17.41.138:8030")
                        .withProperty("database-name", "nuc_db")
                        .withProperty("username", "huquan")
                        .withProperty("password", "oNa46nj0o65b@kvK")
                        .withProperty("table-name", "upload_log_city")
                        .withProperty("sink.properties.format", "json")
                        .withProperty("sink.properties.strip_outer_array", "true")
                        // TODO 删除测试代码
                        .withProperty("sink.buffer-flush.interval-ms", "1000")
                        // 设置并行度，多并行度情况下需要考虑如何保证数据有序性
                        .withProperty("sink.parallelism", "1")
                        .build()
        );
        resultStream.print("result");
        resultStream.addSink(srSink);
        try {
            env.execute("huquan_CityDistrictDataStat");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * report数据统计
     *
     * @param tEnv
     * @return
     */
    private static Table _statReport(StreamTableEnvironment tEnv) {
        String sql = "select " +
                "CONCAT_WS('_',cast(areaId as string),collectLocationCity,collectLocationDistrict,'4',cast(numberReport as string)) union_id, " +
                "areaId area_id, " + // 区域编码
                "collectLocationCity collect_location_city, " + // 采样点市
                "collectLocationDistrict collect_location_district, " + // 采样点区县
                "4 source, " + // 上传环节：1采样 2转运 3接收 4结果
//                "my_concat(concat_ws('_',personIdCard,tubeCode)) upload_number, " + // 将personIdCard和tubeCode用"_"连接，并将分组内多个连接字符串用","连接，用于后续计算采检一致指标
                "upload_number(concat_ws('_',personIdCard,tubeCode)) upload_number, " + // 将personIdCard和tubeCode用"_"连接，并将分组内多个连接字符串用","连接，用于后续计算采检一致指标
                "count(distinct CONCAT(cast(collectTime as string),personIdCard) ) success_number, " + // 不重复上传数量
                "0 fail_number, " + // 环节对应数量:为空
                "count(distinct tubeCode) tube_number, " +
//                "'0' problem_number, " + // TODO 有工单数据量(二次校验不通过数量)
                "report_check(personIdCard, personName, personPhone, packTime, collectTime, receiveTime, checkTime, addTime, collectCount, collectLimitnum, tubeCode) problem_number, " + // TODO 有工单数据量(二次校验不通过数量)
                "numberReport problem_number_report, " +
                "0 problem_error, " + // TODO 数据异常问题量(二次校验问题分组统计)
                "0 problem_unsync, " + // TODO 环节不对应问题量(二次校验问题分组统计)
                "max(interfaceRecTime) upload_time, " + // 上传时间(接口接收时间)
                "max(userName) create_by " + // 上传用户名(多个userName默认取第一个)
                "from report_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId, " +
                "collectLocationCity , " +
                "collectLocationDistrict, " +
                "numberReport";
        return tEnv.sqlQuery(sql);
    }

    /**
     * receive数据统计
     *
     * @param tEnv
     * @return
     */
    private static Table _statReceive(StreamTableEnvironment tEnv) {
        String sql = "select " +
                "CONCAT_WS('_',cast(areaId as string),'','','3',cast(numberReport as string)) union_id, " +
                "areaId area_id, " + // 区域编码
                "cast('' as string) collect_location_city, " + // 采样点市
                "cast('' as string) collect_location_district, " + // 采样点区县
                "3 source, " + // 上传环节：1采样 2转运 3接收 4结果
                "0 upload_number, " +
                "cast(0 as bigint) success_number, " + // 不重复上传数量
                "0 fail_number, " + // 环节对应数量:为空
                "cast(sum(tubeNum) as bigint) tube_number, " +
                "'0,0,0' problem_number, " + // TODO 有工单数据量(二次校验不通过数量)
                "numberReport problem_number_report, " +
                "0 problem_error, " + // TODO 数据异常问题量(二次校验问题分组统计)
                "0 problem_unsync, " + // TODO 环节不对应问题量(二次校验问题分组统计)
                "max(interfaceRecTime) upload_time, " + // 上传时间(接口接收时间)
                "max(userName) create_by " + // TODO 上传用户名(多个userName默认取第一个)
                "from receive_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId, " +
                "numberReport";
        return tEnv.sqlQuery(sql);
    }


    /**
     * transport数据统计
     *
     * @param tEnv
     * @return
     */
    private static Table _statTransport(StreamTableEnvironment tEnv) {
        String sql = "select " +
                "CONCAT_WS('_',cast(areaId as string),'','','2',cast(numberReport as string)) union_id, " +
                "areaId area_id, " + // 区域编码
                "cast('' as string) collect_location_city, " + // 采样点市
                "cast('' as string) collect_location_district, " + // 采样点区县
                "2 source, " + // 上传环节：1采样 2转运 3接收 4结果
                "0 upload_number, " +
                "cast(0 as bigint) success_number, " + // 不重复上传数量
                "0 fail_number, " + // 环节对应数量:为空
                "cast(sum(tubeNum) as bigint) tube_number, " +
                "'0,0,0' problem_number, " + // TODO 有工单数据量(二次校验不通过数量)
                "numberReport problem_number_report, " +
                "0 problem_error, " + // TODO 数据异常问题量(二次校验问题分组统计)
                "0 problem_unsync, " + // TODO 环节不对应问题量(二次校验问题分组统计)
                "max(interfaceRecTime) upload_time, " + // 上传时间(接口接收时间)
                "max(userName) create_by " + // 上传用户名(多个userName默认取第一个)
                "from transport_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId, " +
                "numberReport";
        return tEnv.sqlQuery(sql);
    }

    /**
     * collect数据统计
     *
     * @param tEnv
     * @return
     */
    private static Table _statCollect(StreamTableEnvironment tEnv) {
        String collectStatSql = "select " +
                "CONCAT_WS('_',cast(areaId as string),collectLocationCity,collectLocationDistrict,'1',cast(numberReport as string)) union_id, " +
                "areaId area_id, " + // 区域编码
                "collectLocationCity collect_location_city, " + // 采样点市
                "collectLocationDistrict collect_location_district, " + // 采样点区县
                "1 source, " + // 上传环节：1采样 2转运 3接收 4结果
                "0 upload_number, " +
                "count(distinct CONCAT(cast(collectTime as string),personIdCard) ) success_number, " + // 不重复上传数量
                "0 fail_number, " + // 环节对应数量:为空
                "count(distinct tubeCode) tube_number, " +
//                "problem_number_stat('1',personIdCard,personPhone,personName,collectCount,collectLimitnum," +
//                "addTime,collectTime,'','','') problem_number, " +
                "collect_check(personName,personPhone,personIdCard,collectCount,collectLimitnum,addTime,collectTime,tubeCode) problem_number, " + // 有工单数据量(二次校验不通过数量)
                "numberReport problem_number_report, " +
                "0 problem_error, " + // 数据异常问题量(二次校验问题分组统计)。统一在problem_number中计算
                "0 problem_unsync, " + // 环节不对应问题量(二次校验问题分组统计)。统一在problem_number中计算
                "max(interfaceRecTime) upload_time, " + // 上传时间(接口接收时间)
                "max(userName) create_by " + // 上传用户名(多个userName默认取第一个)
                "from collect_data " +
                "group by " +
                "TUMBLE(pt, INTERVAL '3' SECOND), " +
                "areaId, " +
                "collectLocationCity , " +
                "collectLocationDistrict, " +
                "numberReport";
        return tEnv.sqlQuery(collectStatSql);
    }
}
