package com.sxhs.realtime.test;

import com.sxhs.realtime.bean.ReportDataId;
import lombok.Data;

@Data
public class TestReportData extends ReportDataId {
    private long timestamp;
}
