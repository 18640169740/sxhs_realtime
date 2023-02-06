package com.sxhs.realtime.util;

import com.sxhs.realtime.bean.ProblemDataCr;
import com.sxhs.realtime.bean.ReportDataId;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReportCheckUdf extends AggregateFunction<String, SecondCheckAcc<ReportDataId>> {
    private final static String DEFAULT = "0,0,0";

    public void accumulate(SecondCheckAcc<ReportDataId> acc, String personIdCard, String personName, String personPhone, String packTime, String collectTime, String receiveTime, String checkTime, String addTime, Integer collectCount, Integer collectLimitnum, String tubeCode) {
        acc.add(new ReportDataId(personIdCard, personName, personPhone, packTime, collectTime, receiveTime, checkTime, addTime, collectCount, collectLimitnum, tubeCode));
    }

    /**
     * @param acc the accumulator which contains the current intermediate results
     * @return 返回二次校验不通过计数。"二次校验不通过数量，数据异常问题量，环节不对应问题量"
     */
    @Override
    public String getValue(SecondCheckAcc<ReportDataId> acc) {
        List<ReportDataId> dataList = acc.getDataList();
        if (dataList != null && dataList.size() > 0) {
            Table table = null;
            try {
                table = HbaseUtil.getTable("nuc_relation_distinct");
                List<List<ProblemDataCr>> list = new ArrayList<>();
                for (ReportDataId report : dataList) {
                    Tuple2<List<ProblemDataCr>, List<ProblemDataCr>> result = NucCheckUtil.reportCheck(report, table);
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
        return new SecondCheckAcc<ReportDataId>();
    }
}
