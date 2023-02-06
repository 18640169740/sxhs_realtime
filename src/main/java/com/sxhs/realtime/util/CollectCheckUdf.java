package com.sxhs.realtime.util;

import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ProblemDataCr;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CollectCheckUdf extends AggregateFunction<String, SecondCheckAcc<CollectDataId>> {
    private final static String DEFAULT = "0,0,0";

    public void accumulate(SecondCheckAcc<CollectDataId> acc, String personName, String personPhone, String personIdCard, Integer collectCount, Integer collectLimitnum, String addTime, String collectTime, String tubeCode) {
        acc.add(new CollectDataId(personName, personPhone, personIdCard, collectCount, collectLimitnum, addTime, collectTime, tubeCode));
    }

    /**
     * @param acc the accumulator which contains the current intermediate results
     * @return 返回二次校验不通过计数。"二次校验不通过数量，数据异常问题量，环节不对应问题量"
     */
    @Override
    public String getValue(SecondCheckAcc<CollectDataId> acc) {
        List<CollectDataId> dataList = acc.getDataList();
        if (dataList != null && dataList.size() > 0) {
            Table table = null;
            try {
                table = HbaseUtil.getTable("nuc_relation_distinct");
                List<List<ProblemDataCr>> list = new ArrayList<>();
                for (CollectDataId collect : dataList) {
                    Tuple2<List<ProblemDataCr>, List<ProblemDataCr>> result = NucCheckUtil.collectCheck(collect, table);
                    list.add(result.f0);
                }
                return acc.getResult(list);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (table != null) {
                    try {
                        table.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
        return DEFAULT;
    }

    @Override
    public SecondCheckAcc createAccumulator() {
        return new SecondCheckAcc<CollectDataId>();
    }
}
