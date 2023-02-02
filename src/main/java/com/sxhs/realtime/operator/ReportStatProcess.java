package com.sxhs.realtime.operator;

import com.sxhs.realtime.bean.HourSumReportId;
import com.sxhs.realtime.common.Constants;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @Description: 上报统计信息统计
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 17:24
 */
public class ReportStatProcess extends KeyedProcessFunction<Tuple3<String,String,String>, HourSumReportId, HourSumReportId> {

    /**
     * state设置
     */
    private transient MapState<String, Map<String,String>> mapState;
    //state ttl
    private int stateDays = 2;
    //state name
    private String stateName = "reportStatState";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //set state
        MapStateDescriptor<String, Map<String,String>> descriptor =
                new MapStateDescriptor<>(
                        stateName,
                        Types.STRING,
                        Types.MAP(Types.STRING,Types.STRING));
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(stateDays))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(HourSumReportId hourSumReportId, Context context, Collector<HourSumReportId> collector) throws Exception {
        StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
        if(hourSumReportId.getAreaId() == null || hourSumReportId.getCollectDate() == null){
            return;
        }
        sj.add(hourSumReportId.getAreaId().toString());
        sj.add(hourSumReportId.getCollectDate());
        sj.add(String.valueOf(hourSumReportId.getUnitHour()));
        String key = sj.toString();
        Map<String, String> result = mapState.get(key);
        if (result == null || result.size() == 0) {
            result = new HashMap<>();
            result.put("createTime",hourSumReportId.getCreateTime());
            result.put("id",hourSumReportId.getId().toString());
            result.put("collectPersons",String.valueOf(hourSumReportId.getCollectPersons()));
            result.put("collectSampleNum",String.valueOf(hourSumReportId.getCollectSampleNum()));
            result.put("transferPersons",String.valueOf(hourSumReportId.getTransferPersons()));
            result.put("transferSampleNum",String.valueOf(hourSumReportId.getTransferSampleNum()));
            result.put("receivePersons",String.valueOf(hourSumReportId.getReceivePersons()));
            result.put("receiveSampleNum",String.valueOf(hourSumReportId.getReceiveSampleNum()));
            result.put("checkPersons",String.valueOf(hourSumReportId.getCheckPersons()));
            result.put("checkSampleNum",String.valueOf(hourSumReportId.getCheckSampleNum()));
        }else{
            String createTime = result.get("createTime");
            Long id = Long.parseLong(result.get("id"));
            int collectPersons = Integer.parseInt(result.get("collectPersons"))
                    + hourSumReportId.getCollectPersons();
            int collectSampleNum = Integer.parseInt(result.get("collectSampleNum"))
                    + hourSumReportId.getCollectSampleNum();
            int transferPersons = Integer.parseInt(result.get("transferPersons"))
                    + hourSumReportId.getTransferPersons();
            int transferSampleNum = Integer.parseInt(result.get("transferSampleNum"))
                    + hourSumReportId.getTransferSampleNum();
            int receivePersons = Integer.parseInt(result.get("receivePersons"))
                    + hourSumReportId.getReceivePersons();
            int receiveSampleNum =  Integer.parseInt(result.get("receiveSampleNum"))
                    + hourSumReportId.getReceiveSampleNum();
            int checkPersons = Integer.parseInt(result.get("checkPersons"))
                    + hourSumReportId.getCheckPersons();
            int checkSampleNum = Integer.parseInt(result.get("checkSampleNum"))
                    + hourSumReportId.getCheckSampleNum();
            hourSumReportId.setCreateTime(createTime);
            hourSumReportId.setId(id);
            hourSumReportId.setCollectPersons(collectPersons);
            hourSumReportId.setCollectSampleNum(collectSampleNum);
            hourSumReportId.setTransferPersons(transferPersons);
            hourSumReportId.setTransferSampleNum(transferSampleNum);
            hourSumReportId.setReceivePersons(receivePersons);
            hourSumReportId.setReceiveSampleNum(receiveSampleNum);
            hourSumReportId.setCheckPersons(checkPersons);
            hourSumReportId.setCheckSampleNum(checkSampleNum);

            result.put("collectPersons",String.valueOf(collectPersons));
            result.put("collectSampleNum",String.valueOf(collectSampleNum));
            result.put("transferPersons",String.valueOf(transferPersons));
            result.put("transferSampleNum",String.valueOf(transferSampleNum));
            result.put("receivePersons",String.valueOf(receivePersons));
            result.put("receiveSampleNum",String.valueOf(receiveSampleNum));
            result.put("checkPersons",String.valueOf(checkPersons));
            result.put("checkSampleNum",String.valueOf(checkSampleNum));
        }
        mapState.put(key,result);
        collector.collect(hourSumReportId);
    }
}
