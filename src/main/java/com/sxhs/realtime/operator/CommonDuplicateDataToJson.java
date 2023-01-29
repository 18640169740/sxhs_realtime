package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CommonDuplicateData;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Description:
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/28 17:47
 */
public class CommonDuplicateDataToJson implements MapFunction<CommonDuplicateData, String> {
    @Override
    public String map(CommonDuplicateData commonDuplicateData) throws Exception {
        String jsonStr = JSONObject.toJSONString(commonDuplicateData);
        return jsonStr;
    }
}
