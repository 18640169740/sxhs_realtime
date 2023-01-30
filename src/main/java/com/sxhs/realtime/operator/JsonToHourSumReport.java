package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CommonDuplicateData;
import com.sxhs.realtime.bean.HourSumReportId;
import com.sxhs.realtime.common.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @Description: json数据 -> HourSumReportId
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 16:51
 */
public class JsonToHourSumReport implements FlatMapFunction<String, HourSumReportId> {
    @Override
    public void flatMap(String str, Collector<HourSumReportId> collector) throws Exception {
        if (StringUtils.isBlank(str)) {
            return;
        }
        JSONObject jsonObject = JSONObject.parseObject(str);
        String type = jsonObject.getString("type");
        if("HOUR_SUM_REPORT".equalsIgnoreCase(type)){
            JSONArray data = jsonObject.getJSONArray("data");
            data.forEach(obj -> {
                JSONObject dataJsonObject = (JSONObject) obj;
                HourSumReportId hourSumReportId = JSON.parseObject(dataJsonObject.toJSONString(), HourSumReportId.class);
                hourSumReportId.setCreateBy(jsonObject.getString("userName"));
                hourSumReportId.setCreateTime(Constants.FASTDATEFORMAT.format(new Date()));
                hourSumReportId.setUpdateTime(Constants.FASTDATEFORMAT.format(new Date()));
                hourSumReportId.setIsDelete(0);
                collector.collect(hourSumReportId);
            });
        }
    }
}
