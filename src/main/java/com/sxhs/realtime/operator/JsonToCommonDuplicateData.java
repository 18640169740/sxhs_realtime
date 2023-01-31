package com.sxhs.realtime.operator;

import com.alibaba.fastjson.JSON;
import com.sxhs.realtime.bean.CommonDuplicateData;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Description: 数据解析（json -> CommonDuplicateData）
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/28 14:45
 */
public class JsonToCommonDuplicateData implements FlatMapFunction<String, CommonDuplicateData> {
    @Override
    public void flatMap(String str, Collector<CommonDuplicateData> collector) throws Exception {
        if (StringUtils.isBlank(str)) {
            return;
        }
        CommonDuplicateData commonDuplicateData = JSON.parseObject(str, CommonDuplicateData.class);

        if(commonDuplicateData != null && StringUtils.isNotBlank(commonDuplicateData.getType())){
            collector.collect(commonDuplicateData);
        }
    }
}
