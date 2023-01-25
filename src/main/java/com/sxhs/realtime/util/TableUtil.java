package com.sxhs.realtime.util;

import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableUtil {
    public static Table collectTable(StreamTableEnvironment tEnv, SingleOutputStreamOperator<CollectDataId> collectStream) {
        return tEnv.fromDataStream(collectStream,
                $("id"),
                $("submitId"),
                $("areaId"),
                $("recordId"),
                $("personIdCard"),
                $("personIdCardType"),
                $("personName"),
                $("personPhone"),
                $("collectLocationName"),
                $("collectLocationProvince"),
                $("collectLocationCity"),
                $("collectLocationDistrict"),
                $("collectLocationStreet"),
                $("collectOrgName"),
                $("personId"),
                $("collectTypeId"),
                $("collectPartId"),
                $("tubeCode"),
                $("collectUser"),
                $("collectTime"),
                $("addTime"),
                $("collectCount"),
                $("collectLimitnum"),
                $("numberReport"),
                $("interfaceRecTime"),
                $("userName"),
                $("clientId"),
                $("pt").proctime());
    }

    public static Table transportTable(StreamTableEnvironment tEnv, DataStream<TransportDataId> tranportStream) {
        return tEnv.fromDataStream(tranportStream,
                $("id"),
                $("submitId"),
                $("areaId"),
                $("deliveryCode"),
                $("deliveryName"),
                $("deliveryTime"),
                $("packTime"),
                $("collectAddress"),
                $("deliveryStatus"),
                $("deliveryOrgName"),
                $("deliveryPrp"),
                $("transportOrgName"),
                $("transportPrp"),
                $("receiveOrgName"),
                $("receiveOrgType"),
                $("receivePrp"),
                $("addTime"),
                $("tubeNum"),
                $("packNum"),
                $("transportItem"),
                $("numberReport"),
                $("interfaceRecTime"),
                $("userName"),
                $("clientId"),
                $("pt").proctime());
    }

    public static Table receiveTable(StreamTableEnvironment tEnv, DataStream<ReceiveDataId> receiveStream) {
        return tEnv.fromDataStream(receiveStream,
                $("id"),
                $("submitId"),
                $("areaId"),
                $("receiveCode"),
                $("receiveName"),
                $("receiveTime"),
                $("deliveryCode"),
                $("deliveryOrgName"),
                $("deliveryPrp"),
                $("deliveryPrpId"),
                $("deliveryPrpPhone"),
                $("transportOrgName"),
                $("transportPrp"),
                $("transportPrpId"),
                $("transportPrpPhone"),
                $("receiveOrgName"),
                $("receiveOrgType"),
                $("receivePrp"),
                $("addTime"),
                $("numberReport"),
                $("tubeNum"),
                $("packNum"),
                $("receivesItem"),
                $("interfaceRecTime"),
                $("userName"),
                $("clientId"),
                $("pt").proctime());
    }

    public static Table reportTable(StreamTableEnvironment tEnv, DataStream<ReportDataId> reportStream) {
        return tEnv.fromDataStream(reportStream,
                $("id"),
                $("submitId"),
                $("areaId"),
                $("recordId"),
                $("personIdCard"),
                $("personIdCardType"),
                $("personName"),
                $("personPhone"),
                $("collectLocationName"),
                $("collectLocationProvince"),
                $("collectLocationCity"),
                $("collectLocationDistrict"),
                $("collectLocationStreet"),
                $("collectTypeId"),
                $("collectPartId"),
                $("tubeCode"),
                $("collectUser"),
                $("collectTime"),
                $("packTime"),
                $("deliveryCode"),
                $("collectOrgName"),
                $("checkOrgName"),
                $("receiveTime"),
                $("receiveUser"),
                $("personId"),
                $("checkTime"),
                $("checkUser"),
                $("checkResult"),
                $("iggResult"),
                $("igmResult"),
                $("remark"),
                $("addTime"),
                $("collectLimitnum"),
                $("collectCount"),
                $("numberReport"),
                $("interfaceRecTime"),
                $("userName"),
                $("clientId"),
                $("pt").proctime());
    }
}