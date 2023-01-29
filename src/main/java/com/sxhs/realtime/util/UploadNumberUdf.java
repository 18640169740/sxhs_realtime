package com.sxhs.realtime.util;

import org.apache.flink.table.functions.AggregateFunction;

public class UploadNumberUdf extends AggregateFunction<Integer, HbaseAcc> {
    public void accumulate(HbaseAcc acc, String str) {
        acc.addElement(str);
    }

    @Override
    public Integer getValue(HbaseAcc acc) {
        return acc.getValue();
    }

    @Override
    public HbaseAcc createAccumulator() {
        return new HbaseAcc();
    }

}
