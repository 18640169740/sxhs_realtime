package com.sxhs.realtime.test;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;

import java.sql.Timestamp;
import java.time.Instant;

public class DiffUdf extends ScalarFunction {
    @DataTypeHint()
    public long eval(Timestamp time1, Timestamp time2) {
        return time2.getTime() - time1.getTime();
    }
}
