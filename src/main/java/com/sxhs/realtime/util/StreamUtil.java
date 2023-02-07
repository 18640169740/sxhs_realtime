package com.sxhs.realtime.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.CollectDataId;
import com.sxhs.realtime.bean.ReceiveDataId;
import com.sxhs.realtime.bean.ReportDataId;
import com.sxhs.realtime.bean.TransportDataId;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamUtil {
    private static final Logger logger = LoggerFactory.getLogger(StreamUtil.class);

    /**
     * 通过侧输出流，将collect、transport、receive、report数据过滤封装成流
     *
     * @param sourceStream kafka NUC_DATA_ID数据
     * @return
     */
    public static Tuple4<SingleOutputStreamOperator<CollectDataId>, DataStream<TransportDataId>,
            DataStream<ReceiveDataId>, DataStream<ReportDataId>> sideOutput(DataStreamSource<String> sourceStream) {
        OutputTag<TransportDataId> transportTag = new OutputTag<TransportDataId>("transportTag") {
        };
        OutputTag<ReceiveDataId> receiveTag = new OutputTag<ReceiveDataId>("receiveTag") {
        };
        OutputTag<ReportDataId> reportTag = new OutputTag<ReportDataId>("reportTag") {
        };
        SingleOutputStreamOperator<CollectDataId> collectStream = sourceStream.process(new ProcessFunction<String, CollectDataId>() {
            @Override
            public void processElement(String value, ProcessFunction<String, CollectDataId>.Context ctx, Collector<CollectDataId> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(value);
                    String type = jsonObj.getString("type");
                    if (type != null) {
                        switch (type) {
                            case "COLLECT_DATA":
                                _toCollectId(jsonObj, collector);
                                break;
                            case "TRANSPORT_DATA":
                                _toTransportId(jsonObj, ctx, transportTag);
                                break;
                            case "RECEIVE_DATA":
                                _toReceiveId(jsonObj, ctx, receiveTag);
                                break;
                            case "REPORT_DATA":
                            case "XA_REPORT_DATA":
                                _toReportId(jsonObj, ctx, reportTag);
                                break;
                            default:
//                                logger.error("unknow type {}", type);
                        }
                    } else {
                        logger.error("null type error: {}", value);
                    }
                } catch (Exception e) {
                    logger.error("json parse error: {}", value);
                }
            }
        });

        DataStream<TransportDataId> tranportStream = collectStream.getSideOutput(transportTag);
        DataStream<ReceiveDataId> receiveStream = collectStream.getSideOutput(receiveTag);
        DataStream<ReportDataId> reportStream = collectStream.getSideOutput(reportTag);
        return new Tuple4<>(collectStream, tranportStream, receiveStream, reportStream);
    }

    private static void _toReportId(JSONObject json, ProcessFunction<String, CollectDataId>.Context ctx, OutputTag<ReportDataId> reportTag) {
        JSONArray data = json.getJSONArray("data");
        if (data != null && data.size() > 0) {
            for (int i = 0; i < data.size(); i++) {
                JSONObject result = new JSONObject();
                result.put("numberReport", json.getLong("numberReport"));
                result.put("interfaceRecTime", json.get("interfaceRecTime"));
                result.put("userName", json.get("userName"));
                result.put("clientId", json.get("clientId"));
                result.put("dropDataNum", json.get("dropDataNum"));

                JSONObject jsonObj = data.getJSONObject(i);
                result.put("id", jsonObj.get("id"));
                result.put("submitId", jsonObj.get("submitId"));
                result.put("areaId", jsonObj.get("areaId"));
                result.put("recordId", jsonObj.get("recordId"));
                result.put("personIdCard", jsonObj.get("personIdCard"));
                result.put("personIdCardType", jsonObj.get("personIdCardType"));
                result.put("personName", jsonObj.get("personName"));
                result.put("personPhone", jsonObj.get("personPhone"));
                result.put("collectLocationName", jsonObj.get("collectLocationName"));
                result.put("collectLocationProvince", jsonObj.get("collectLocationProvince"));
                result.put("collectLocationCity", jsonObj.get("collectLocationCity"));
                result.put("collectLocationDistrict", jsonObj.get("collectLocationDistrict"));
                result.put("collectLocationStreet", jsonObj.get("collectLocationStreet"));
                result.put("collectTypeId", jsonObj.get("collectTypeId"));
                result.put("collectPartId", jsonObj.get("collectPartId"));
                result.put("tubeCode", jsonObj.get("tubeCode"));
                result.put("collectUser", jsonObj.get("collectUser"));
                result.put("collectTime", jsonObj.get("collectTime"));
                result.put("packTime", jsonObj.get("packTime"));
                result.put("deliveryCode", jsonObj.get("deliveryCode"));
                result.put("collectOrgName", jsonObj.get("collectOrgName"));
                result.put("checkOrgName", jsonObj.get("checkOrgName"));
                result.put("receiveTime", jsonObj.get("receiveTime"));
                result.put("receiveUser", jsonObj.get("receiveUser"));
                result.put("personId", jsonObj.get("personId"));
                result.put("checkTime", jsonObj.get("checkTime"));
                result.put("checkUser", jsonObj.get("checkUser"));
                result.put("checkResult", jsonObj.get("checkResult"));
                result.put("iggResult", jsonObj.get("iggResult"));
                result.put("igmResult", jsonObj.get("igmResult"));
                result.put("remark", jsonObj.get("remark"));
                result.put("addTime", jsonObj.get("addtime"));
                result.put("collectLimitnum", jsonObj.get("collectLimitnum"));
                result.put("collectCount", jsonObj.get("collectCount"));

                ReportDataId reportDataId = result.toJavaObject(ReportDataId.class);
                ctx.output(reportTag, reportDataId);
            }
        }
    }

    private static void _toReceiveId(JSONObject json, ProcessFunction<String, CollectDataId>.Context ctx, OutputTag<ReceiveDataId> receiveTag) {
        JSONArray data = json.getJSONArray("data");
        if (data != null && data.size() > 0) {
            for (int i = 0; i < data.size(); i++) {
                JSONObject result = new JSONObject();
                result.put("numberReport", json.getLong("numberReport"));
                result.put("interfaceRecTime", json.get("interfaceRecTime"));
                result.put("userName", json.get("userName"));
                result.put("clientId", json.get("clientId"));
                result.put("dropDataNum", json.get("dropDataNum"));

                JSONObject jsonObj = data.getJSONObject(i);
                result.put("id", jsonObj.get("id"));
                result.put("submitId", jsonObj.get("submitId"));
                result.put("areaId", jsonObj.get("areaId"));
                result.put("receiveCode", jsonObj.get("receiveCode"));
                result.put("receiveName", jsonObj.get("receiveName"));
                result.put("receiveTime", jsonObj.get("receiveTime"));
                result.put("deliveryCode", jsonObj.get("deliveryCode"));
                result.put("deliveryOrgName", jsonObj.get("deliveryOrgName"));
                result.put("deliveryPrp", jsonObj.get("deliveryPrp"));
                result.put("deliveryPrpId", jsonObj.get("deliveryPrpId"));
                result.put("deliveryPrpPhone", jsonObj.get("deliveryPrpPhone"));
                result.put("transportOrgName", jsonObj.get("transportOrgName"));
                result.put("transportPrp", jsonObj.get("transportPrp"));
                result.put("transportPrpId", jsonObj.get("transportPrpId"));
                result.put("transportPrpPhone", jsonObj.get("transportPrpPhone"));
                result.put("receiveOrgName", jsonObj.get("receiveOrgName"));
                result.put("receiveOrgType", jsonObj.get("receiveOrgType"));
                result.put("receivePrp", jsonObj.get("receivePrp"));
                result.put("addTime", jsonObj.get("addTime"));
                result.put("tubeNum", jsonObj.get("tubeNum"));
                result.put("packNum", jsonObj.get("packNum"));
                result.put("receivesItem", jsonObj.get("receivesItem"));

                ReceiveDataId receiveDataId = result.toJavaObject(ReceiveDataId.class);
                ctx.output(receiveTag, receiveDataId);
            }
        }
    }

    public static void _toCollectId(JSONObject json, Collector<CollectDataId> collector) {
        JSONArray data = json.getJSONArray("data");
        if (data != null && data.size() > 0) {
            for (int i = 0; i < data.size(); i++) {
                JSONObject result = new JSONObject();
                result.put("numberReport", json.getLong("numberReport"));
                result.put("interfaceRecTime", json.get("interfaceRecTime"));
                result.put("userName", json.get("userName"));
                result.put("clientId", json.get("clientId"));
                result.put("dropDataNum", json.get("dropDataNum"));

                JSONObject jsonObj = data.getJSONObject(i);
                result.put("id", jsonObj.get("id"));
                result.put("submitId", jsonObj.get("submitId"));
                result.put("areaId", jsonObj.get("areaId"));
                result.put("recordId", jsonObj.get("recordId"));
                result.put("personIdCard", jsonObj.get("personIdCard"));
                result.put("personIdCardType", jsonObj.get("personIdCardType"));
                result.put("personName", jsonObj.get("personName"));
                result.put("personPhone", jsonObj.get("personPhone"));
                result.put("collectLocationName", jsonObj.get("collectLocationName"));
                result.put("collectLocationProvince", jsonObj.get("collectLocationProvince"));
                result.put("collectLocationCity", jsonObj.get("collectLocationCity"));
                result.put("collectLocationDistrict", jsonObj.get("collectLocationDistrict"));
                result.put("collectLocationStreet", jsonObj.get("collectLocationStreet"));
                result.put("collectOrgName", jsonObj.get("collectOrgName"));
                result.put("personId", jsonObj.get("personId"));
                result.put("collectTypeId", jsonObj.get("collectTypeId"));
                result.put("collectPartId", jsonObj.get("collectPartId"));
                result.put("tubeCode", jsonObj.get("tubeCode"));
                result.put("collectUser", jsonObj.get("collectUser"));
                result.put("collectTime", jsonObj.get("collectTime"));
                result.put("addTime", jsonObj.get("addTime"));
                result.put("collectCount", jsonObj.get("collectCount"));
                result.put("collectLimitnum", jsonObj.get("collectLimitnum"));
                collector.collect(result.toJavaObject(CollectDataId.class));
            }
        }
    }

    public static void _toTransportId(JSONObject json, ProcessFunction<String, CollectDataId>.Context ctx, OutputTag<TransportDataId> transportTag) {
        JSONArray data = json.getJSONArray("data");
        if (data != null && data.size() > 0) {
            for (int i = 0; i < data.size(); i++) {
                JSONObject result = new JSONObject();
                result.put("numberReport", json.getLong("numberReport"));
                result.put("interfaceRecTime", json.get("interfaceRecTime"));
                result.put("userName", json.get("userName"));
                result.put("clientId", json.get("clientId"));
                result.put("dropDataNum", json.get("dropDataNum"));

                JSONObject jsonObj = data.getJSONObject(i);
                result.put("id", jsonObj.get("id"));
                result.put("submitId", jsonObj.get("submitId"));
                result.put("areaId", jsonObj.get("areaId"));
                result.put("deliveryCode", jsonObj.get("deliveryCode"));
                result.put("deliveryName", jsonObj.get("deliveryName"));
                result.put("deliveryTime", jsonObj.get("deliveryTime"));
                result.put("packTime", jsonObj.get("packTime"));
                result.put("collectAddress", jsonObj.get("collectAddress"));
                result.put("deliveryStatus", jsonObj.get("deliveryStatus"));
                result.put("deliveryOrgName", jsonObj.get("deliveryOrgName"));
                result.put("deliveryPrp", jsonObj.get("deliveryPrp"));
                result.put("transportOrgName", jsonObj.get("transportOrgName"));
                result.put("transportPrp", jsonObj.get("transportPrp"));
                result.put("receiveOrgName", jsonObj.get("receiveOrgName"));
                result.put("receiveOrgType", jsonObj.get("receiveOrgType"));
                result.put("receivePrp", jsonObj.get("receivePrp"));
                result.put("addTime", jsonObj.get("addtime"));
                result.put("tubeNum", jsonObj.get("tubeNum"));
                result.put("packNum", jsonObj.get("packNum"));
                result.put("transportItem", jsonObj.get("transportItem"));

                TransportDataId transportDataId = result.toJavaObject(TransportDataId.class);
                ctx.output(transportTag, transportDataId);
            }
        }
    }

//    public static void main(String[] args) {
////        String jsonStr = "{\"userName\":\"张三\",\"type\":\"COLLECT_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"submitId\":10000,\"areaId\":6101,\"recordId\":100000,\"personIdCard\":\"f6607a3c44368ffa7d9494f596cfe51bcfcc931605dbf80f6e227260fc22d408\",\"personIdCardType\":\"1\",\"personName\":\"8968878e28976abfdc60668e90d8fb34\",\"personPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"collectLocationName\":\"1\",\"collectLocationProvince\":\"61\",\"collectLocationCity\":\"610100000000\",\"collectLocationDistrict\":\"610103000000\",\"collectTypeId\":1,\"collectPartId\":1,\"tubeCode\":\"BNH0001\",\"collectUser\":\"11\",\"collectOrgName\":\"机构\",\"collectTime\":\"2021-11-2214:50:53\",\"addtime\":\"2021-11-22 14:50:53\",\"collectLimitnum\":111,\"collectCount\":222}]}";
////        String jsonStr = "{\"userName\":\"张三\",\"type\":\"TRANSPORT_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"areaId\":6101,\"submitId\":10259104,\"deliveryCode\":\"12341412\",\"deliveryName\":\"运输核酸数据\",\"deliveryTime\":\"2022-08-2015:00:10\",\"packTime\":\"2022-08-2014:56:23\",\"deliveryStatus\":1,\"deliveryOrgName\":\"交付对象公司\",\"deliveryPrp\":\"交付人\",\"transportOrgName\":\"运输公司\",\"transportPrp\":\"运输人\",\"receiveOrgName\":\"接收公司\",\"receiveOrgType\":\"4\",\"receivePrp\":\"接收人\",\"tubeNum\":4,\"packNum\":1,\"addTime\":\"2022-08-2015:08:23\",\"transportItem\":[{\"tubeCode\":\"0448692\",\"packCode\":\"123456\"},{\"tubeCode\":\"H4908395\"}]}]}";
////        String jsonStr = "{\"userName\":\"张三\",\"type\":\"RECEIVE_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"areaId\":6101,\"submitId\":100089936,\"receiveCode\":\"1222452342345301783\",\"receiveName\":\"传递核酸\",\"receiveTime\":\"2022-08-2015:57:36\",\"deliveryCode\":\"12341412\",\"deliveryOrgName\":\"交付对象公司\",\"deliveryPrp\":\"8968878e28976abfdc60668e90d8fb34\",\"deliveryPrpId\":\"f6607a3c44368ffa7d9494f596cfe51bcfcc931605dbf80f6e227260fc22d408\",\"deliveryPrpPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"transportOrgName\":\"运输公司\",\"transportPrp\":\"308aa7382fedb6224f80677d33f4e78b\",\"transportPrpId\":\"169bb68d3c67d026ee6d06eb73ceacae28ae31f870ab94d2285b4466d91e32b6\",\"transportPrpPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"receiveOrgName\":\"接收公司\",\"receiveOrgType\":4,\"receivePrp\":\"308aa7382fedb6224f80677d33f4e78b\",\"addTime\":\"2022-08-2017:21:23\",\"tubeNum\":4,\"packNum\":1,\"receivesItem\":[{\"tubeCode\":\"80448692\",\"packCode\":\"123456\"},{\"tubeCode\":\"84908395\"}]}]}";
//        String jsonStr = "{\"userName\":\"张三\",\"type\":\"REPORT_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"submitId\":10000,\"areaId\":6101,\"recordId\":100000,\"personIdCard\":\"308aa7382fedb6224f80677d33f4e78b\",\"personIdCardType\":\"1\",\"personName\":\"169bb68d3c67d026ee6d06eb73ceacae28ae31f870ab94d2285b4466d91e32b6\",\"personPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"collectLocationName\":\"1\",\"collectLocationProvince\":\"61\",\"collectLocationCity\":\"610100000000\",\"collectLocationDistrict\":\"610103000000\",\"collectTypeId\":1,\"collectPartId\":1,\"tubeCode\":\"BNH0001\",\"collectUser\":\"11\",\"deliveryCode\":\"PSH0001\",\"collectOrgName\":\"11\",\"checkOrgName\":\"11\",\"collectTime\":\"2021-11-2214:50:53\",\"checkTime\":\"2021-11-2214:50:53\",\"addtime\":\"2021-11-2214:50:53\",\"receiveTime\":\"2021-11-2214:50:53\",\"packTime\":\"2021-11-2214:50:53\",\"receiveUser\":\"11\",\"checkUser\":\"1\",\"checkResult\":\"阴性\",\"iggResult\":\"阴性\",\"igmResult\":\"阴性\",\"collectLimitnum\":111,\"collectCount\":2222}]}";
//        JSONObject jsonObj = JSONObject.parseObject(jsonStr);
////        StreamUtil._toCollectId(jsonObj, null);
////        StreamUtil._toTransportId(jsonObj, null, null);
////        StreamUtil._toReceiveId(jsonObj, null, null);
//        StreamUtil._toReportId(jsonObj, null, null);
//    }
}
