package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.common.Constants;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description:
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 18:21
 */
public class ReportLogProcess extends ProcessFunction<String,String> {
    @Override
    public void processElement(String str, Context context, Collector<String> collector) throws Exception {
        if(StringUtils.isBlank(str)){
            return;
        }
        JSONObject jsonObject = JSONObject.parseObject(str);
        String type = jsonObject.getString("type");
        JSONArray data = jsonObject.getJSONArray("data");
        //id
        String id = SnowflakeIdWorker.generateIdReverse();
        //区域编码
        String areaId = "";
        //上传批次号
        String numberReport = jsonObject.getString("numberReport");


        switch (type){
            case Constants.COLLECT_DATA:
                break;
            case Constants.TRANSPORT_DATA:
                break;
            case Constants.RECEIVE_DATA:
                break;
            case Constants.REPORT_DATA:
            case Constants.XA_REPORT_DATA:
                break;
            default:
                break;
        }
    }
}
