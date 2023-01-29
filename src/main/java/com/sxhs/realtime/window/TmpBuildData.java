package com.sxhs.realtime.window;

import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.bean.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TmpBuildData {
    private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        // 采样数据
        // DataStreamSource<String> collectStream = env.fromElements("{\"userName\":\"张三\",\"type\":\"COLLECT_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"submitId\":10000,\"areaId\":6101,\"recordId\":100000,\"personIdCard\":\"f6607a3c44368ffa7d9494f596cfe51bcfcc931605dbf80f6e227260fc22d408\",\"personIdCardType\":\"1\",\"personName\":\"8968878e28976abfdc60668e90d8fb34\",\"personPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"collectLocationName\":\"1\",\"collectLocationProvince\":\"61\",\"collectLocationCity\":\"610100000000\",\"collectLocationDistrict\":\"610103000000\",\"collectTypeId\":1,\"collectPartId\":1,\"tubeCode\":\"BNH0001\",\"collectUser\":\"11\",\"collectOrgName\":\"机构\",\"collectTime\":\"2021-11-2214:50:53\",\"addtime\":\"2021-11-2214:50:53\",\"collectLimitnum\":111,\"collectCount\":222}]}");
        // 转运数据
        // DataStreamSource<String> transportStream = env.fromElements("{\"userName\":\"张三\",\"type\":\"TRANSPORT_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"areaId\":6101,\"submitId\":10259104,\"deliveryCode\":\"12341412\",\"deliveryName\":\"运输核酸数据\",\"deliveryTime\":\"2022-08-2015:00:10\",\"packTime\":\"2022-08-2014:56:23\",\"deliveryStatus\":1,\"deliveryOrgName\":\"交付对象公司\",\"deliveryPrp\":\"交付人\",\"transportOrgName\":\"运输公司\",\"transportPrp\":\"运输人\",\"receiveOrgName\":\"接收公司\",\"receiveOrgType\":\"4\",\"receivePrp\":\"接收人\",\"tubeNum\":4,\"packNum\":1,\"addTime\":\"2022-08-2015:08:23\",\"transportItem\":[{\"tubeCode\":\"0448692\",\"packCode\":\"123456\"},{\"tubeCode\":\"H4908395\"}]}]}");
        // 接收数据
        // DataStreamSource<String> receiveStream = env.fromElements("{\"userName\":\"张三\",\"type\":\"RECEIVE_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"areaId\":6101,\"submitId\":100089936,\"receiveCode\":\"1222452342345301783\",\"receiveName\":\"传递核酸\",\"receiveTime\":\"2022-08-2015:57:36\",\"deliveryCode\":\"12341412\",\"deliveryOrgName\":\"交付对象公司\",\"deliveryPrp\":\"8968878e28976abfdc60668e90d8fb34\",\"deliveryPrpId\":\"f6607a3c44368ffa7d9494f596cfe51bcfcc931605dbf80f6e227260fc22d408\",\"deliveryPrpPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"transportOrgName\":\"运输公司\",\"transportPrp\":\"308aa7382fedb6224f80677d33f4e78b\",\"transportPrpId\":\"169bb68d3c67d026ee6d06eb73ceacae28ae31f870ab94d2285b4466d91e32b6\",\"transportPrpPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"receiveOrgName\":\"接收公司\",\"receiveOrgType\":4,\"receivePrp\":\"308aa7382fedb6224f80677d33f4e78b\",\"addTime\":\"2022-08-2017:21:23\",\"tubeNum\":4,\"packNum\":1,\"receivesItem\":[{\"tubeCode\":\"80448692\",\"packCode\":\"123456\"},{\"tubeCode\":\"84908395\"}]}]}");
        // 地市检测数据
        // DataStreamSource<String> reportStream = env.fromElements("{\"userName\":\"张三\",\"type\":\"REPORT_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"submitId\":10000,\"areaId\":6101,\"recordId\":100000,\"personIdCard\":\"308aa7382fedb6224f80677d33f4e78b\",\"personIdCardType\":\"1\",\"personName\":\"169bb68d3c67d026ee6d06eb73ceacae28ae31f870ab94d2285b4466d91e32b6\",\"personPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"collectLocationName\":\"1\",\"collectLocationProvince\":\"61\",\"collectLocationCity\":\"610100000000\",\"collectLocationDistrict\":\"610103000000\",\"collectTypeId\":1,\"collectPartId\":1,\"tubeCode\":\"BNH0001\",\"collectUser\":\"11\",\"deliveryCode\":\"PSH0001\",\"collectOrgName\":\"11\",\"checkOrgName\":\"11\",\"collectTime\":\"2021-11-2214:50:53\",\"checkTime\":\"2021-11-2214:50:53\",\"addtime\":\"2021-11-2214:50:53\",\"receiveTime\":\"2021-11-2214:50:53\",\"packTime\":\"2021-11-2214:50:53\",\"receiveUser\":\"11\",\"checkUser\":\"1\",\"checkResult\":\"阴性\",\"iggResult\":\"阴性\",\"igmResult\":\"阴性\",\"collectLimitnum\":111,\"collectCount\":2222}]}");
        // 西安检测数据
        // DataStreamSource<String> xaReportStream = env.fromElements("{\"userName\":\"张三\",\"type\":\"XA_REPORT_DATA\",\"clientId\":111,\"numberReport\":\"111111\",\"interfaceRecTime\":\"2023-01-01 00:00:00\",\"dropDataNum\":100,\"data\":[{\"submitId\":10000,\"areaId\":6101,\"recordId\":100000,\"personIdCard\":\"308aa7382fedb6224f80677d33f4e78b\",\"personIdCardType\":\"1\",\"personName\":\"169bb68d3c67d026ee6d06eb73ceacae28ae31f870ab94d2285b4466d91e32b6\",\"personPhone\":\"67be37b261cb20025c6a33e4ce13972a\",\"collectLocationName\":\"1\",\"collectLocationProvince\":\"61\",\"collectLocationCity\":\"610100000000\",\"collectLocationDistrict\":\"610103000000\",\"collectTypeId\":1,\"collectPartId\":1,\"tubeCode\":\"BNH0001\",\"collectUser\":\"11\",\"deliveryCode\":\"PSH0001\",\"collectOrgName\":\"11\",\"checkOrgName\":\"11\",\"collectTime\":\"2021-11-2214:50:53\",\"checkTime\":\"2021-11-2214:50:53\",\"addtime\":\"2021-11-2214:50:53\",\"receiveTime\":\"2021-11-2214:50:53\",\"packTime\":\"2021-11-2214:50:53\",\"receiveUser\":\"11\",\"checkUser\":\"1\",\"checkResult\":\"阴性\",\"iggResult\":\"阴性\",\"igmResult\":\"阴性\",\"collectLimitnum\":111,\"collectCount\":2222}]}");
//        String collectDataJson = _buildCollectData();
//        System.out.println(collectDataJson);
        String receiveDataJson = _buildTransportData();
        System.out.println(receiveDataJson);
//        String receiveDataJson = _buildReceiveData();
//        System.out.println(receiveDataJson);
//        String reportDataJson = _buildReportData();
//        System.out.println(reportDataJson);
    }

    private static String _buildReceiveData() {
        ReceiveDataId rd = new ReceiveDataId();
        rd.setId(123456l);
        rd.setSubmitId(100089936l);
        rd.setAreaId(6101l);
        rd.setReceiveCode("1222452342345301783");
        rd.setReceiveName("传递核酸");
        rd.setReceiveTime("2022-08-20 15:57:36");
        rd.setDeliveryCode("12341412l");
        rd.setDeliveryOrgName("交付对象公司");
        rd.setDeliveryPrp("8968878e28976abfdc60668e90d8fb34");
        rd.setDeliveryPrpId("f6607a3c44368ffa7d9494f596cfe51bcfcc931605dbf80f6e227260fc22d408");
        rd.setDeliveryPrpPhone("67be37b261cb20025c6a33e4ce13972a");
        rd.setTransportOrgName("运输公司");
        rd.setTransportPrp("308aa7382fedb6224f80677d33f4e78b");
        rd.setTransportPrpId("169bb68d3c67d026ee6d06eb73ceacae28ae31f870ab94d2285b4466d91e32b6");
        rd.setTransportPrpPhone("67be37b261cb20025c6a33e4ce13972a");
        rd.setReceiveOrgName("接收公司");
        rd.setReceiveOrgType(4);
        rd.setReceivePrp("308aa7382fedb6224f80677d33f4e78b");
        rd.setAddTime("2022-08-20 17:21:23");
        rd.setNumberReport(111111);
        rd.setTubeNum(4);
        rd.setPackNum(1);
        rd.setInterfaceRecTime("2023-01-01 00:00:00");
        rd.setReceivesItem(new TransportItem[]{
                new TransportItem(123l, "80448692", "123456"),
                new TransportItem(456l, "84908395", "")
        });
        return JSONObject.toJSONString(rd);
    }

    private static String _buildReportData() throws ParseException {
        ReportDataId rd = new ReportDataId();
        rd.setId(123456l);
        rd.setSubmitId(10000l);
        rd.setAreaId(6101l);
        rd.setRecordId(100000l);
        rd.setPersonIdCard("308aa7382fedb6224f80677d33f4e78b");
        rd.setPersonIdCardType("1");
        rd.setPersonName("169bb68d3c67d026ee6d06eb73ceacae28ae31f870ab94d2285b4466d91e32b6");
        rd.setPersonPhone("67be37b261cb20025c6a33e4ce13972a");
        rd.setCollectLocationName("1");
        rd.setCollectLocationProvince("61");
        rd.setCollectLocationCity("610100000000");
        rd.setCollectLocationDistrict("610103000000");
        rd.setCollectLocationStreet("样例数据无此字段");
        rd.setCollectTypeId(1);
        rd.setCollectPartId(1);
        rd.setTubeCode("BNH0001");
        rd.setCollectUser("11");
        rd.setCollectTime("2021-11-2214:50:53");
        rd.setPackTime("2021-11-2214:50:53");
        rd.setDeliveryCode("PSH0001");
        rd.setCollectOrgName("11");
        rd.setCheckOrgName("11");
        rd.setReceiveTime("2021-11-22 14:50:53");
        rd.setReceiveUser("11");
        rd.setPersonId("样例数据无此字段");
        rd.setCheckTime("2021-11-22 14:50:53");
        rd.setCheckUser("1");
        rd.setCheckResult("阴性");
        rd.setIggResult("阴性");
        rd.setIgmResult("阴性");
        rd.setRemark("样例数据无此字段");
        rd.setAddTime("2021-11-22 14:50:53");
        rd.setCollectLimitnum(111);
        rd.setCollectCount(2222);
        rd.setNumberReport(111111);
        rd.setInterfaceRecTime("2023-01-01 00:00:00");
        return JSONObject.toJSONString(rd);
    }

    private static String _buildTransportData() throws ParseException {
        TransportDataId tp = new TransportDataId();
        tp.setId(123456l);
        tp.setSubmitId(10259104l);
        tp.setAreaId(6101l);
        tp.setDeliveryCode("12341412");
        tp.setDeliveryName("运输核酸数据");
        tp.setDeliveryTime("2022-08-20 15:00:10");
        tp.setPackTime("2022-08-20 14:56:23");
        tp.setCollectAddress(""); // 样例数据中无此字段
        tp.setDeliveryStatus(1);
        tp.setDeliveryOrgName("交付对象公司");
        tp.setDeliveryPrp("交付人");
        tp.setTransportOrgName("运输公司");
        tp.setTransportPrp("运输人");
        tp.setReceiveOrgName("接收公司");
        tp.setReceiveOrgType(4);
        tp.setReceivePrp("接收人");
        tp.setAddTime("2022-08-20 15:08:23");
        tp.setTubeNum(4);
        tp.setPackNum(1);
        TransportItem ti1 = new TransportItem(1L, "0448692", "123456");
        TransportItem ti2 = new TransportItem(2L, "H4908395", "");
        tp.setTransportItem(new TransportItem[]{ti1, ti2});
        tp.setNumberReport(111111);
        tp.setInterfaceRecTime("2023-01-01 00:00:00");
        return JSONObject.toJSONString(tp);
    }

    private static String _buildCollectData() throws ParseException {
        CollectDataId collectDataId = new CollectDataId();
        collectDataId.setId(1l);
        collectDataId.setSubmitId(10000l);
        collectDataId.setAreaId(6101l);
        collectDataId.setRecordId(100000l);
        collectDataId.setPersonIdCard("f6607a3c44368ffa7d9494f596cfe51bcfcc931605dbf80f6e227260fc22d408");
        collectDataId.setPersonIdCardType("1");
        collectDataId.setPersonName("8968878e28976abfdc60668e90d8fb34");
        collectDataId.setPersonPhone("67be37b261cb20025c6a33e4ce13972a");
        collectDataId.setCollectLocationName("1");
        collectDataId.setCollectLocationProvince("61");
        collectDataId.setCollectLocationCity("610100000000");
        collectDataId.setCollectLocationDistrict("610103000000");
        collectDataId.setCollectLocationStreet("collectLocationStreet");
        collectDataId.setCollectOrgName("机构");
        collectDataId.setPersonId("personId");
        collectDataId.setCollectTypeId(1);
        collectDataId.setCollectPartId(1);
        collectDataId.setTubeCode("BNH0001");
        collectDataId.setCollectUser("11");
        collectDataId.setCollectTime("2021-11-22 14:50:53");
        collectDataId.setAddTime("2021-11-22 14:50:53");
        collectDataId.setCollectCount(222);
        collectDataId.setCollectLimitnum(111);
        collectDataId.setNumberReport(1111);
        collectDataId.setInterfaceRecTime("2023-01-01 00:00:00");
        return JSONObject.toJSONString(collectDataId);
    }
}
