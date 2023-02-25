package com.sxhs.realtime.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sxhs.realtime.operator.FlinkVariablePartitioner;
import com.sxhs.realtime.util.AESUtil;
import com.sxhs.realtime.util.SnowflakeIdWorker;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Optional;
import java.util.Properties;

public class ProductMsg {
    private static final String ZERO = "0";
    private static final String CONCAT_STR = "_";

    public static void main(String[] args) throws Exception {
        int explode = 2700;
//        int explode = 3;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.17.41.132:9091,10.17.41.133:9091,10.17.41.134:9091");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-product_msg_01");
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>("NUC_DATA", new SimpleStringSchema(), properties);
        input.setStartFromGroupOffsets();
//        input.setStartFromEarliest();

        DataStreamSource<String> kafkaStream = env.addSource(input);
//        DataStreamSource<String> kafkaStream = env.fromElements("{\"clientId\":\"304333950247116800\",\"data\":[{\"personIdCardType\":\"1\",\"checkResult\":\"阴性\",\"recordId\":1622761872363077632,\"personPhone\":\"hLeEHnidvsW2K/M0auXXoQ==\",\"collectLimitnum\":1,\"collectPartId\":1,\"collectOrgName\":\"\",\"collectLocationCity\":\"榆林市\",\"submitId\":1622761872363077632,\"deliveryCode\":\"\",\"collectTime\":\"2023-02-07 08:59:14\",\"collectUser\":\"\",\"collectLocationDistrict\":\"2052\",\"personIdCard\":\"aAjZXlfWILW00DLF1kaQg4C54v2oMaVpOZ/+Tj2ZN9I=\",\"checkOrgName\":\"榆林市中医医院\",\"igmResult\":\"阴性\",\"collectCount\":1,\"receiveUser\":\"\",\"personName\":\"vn1oCbAK+yw5iQzRHXwAPg==\",\"receiveTime\":\"2023-02-07 14:19:10\",\"collectLocationName\":\"公交公司火车站核酸采样点\",\"checkUser\":\"张丽\",\"areaId\":6108,\"checkTime\":\"2023-02-07 15:12:14\",\"collectLocationProvince\":\"陕西省\",\"addtime\":\"2023-02-07 15:20:02\",\"iggResult\":\"阴性\",\"personId\":\"\",\"tubeCode\":\"HJ61317558\",\"collectTypeId\":1,\"packTime\":\"\",\"numberReport\":\"385587496656314368\"}],\"interfaceRecTime\":\"2023-02-07 15:24:25\",\"dropDataNum\":0,\"type\":\"REPORT_DATA\",\"userName\":\"yl_hsjcreport\",\"numberReport\":\"385587496656314368\"}");
        SingleOutputStreamOperator<String> resultStream = kafkaStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(s);
                    if ("REPORT_DATA".equals(jsonObj.getString("type"))) {
                        String clientId = jsonObj.getString("clientId");
                        String interfaceRecTime = jsonObj.getString("interfaceRecTime");
                        String type = jsonObj.getString("type");
                        String userName = jsonObj.getString("userName");
                        String numberReport = jsonObj.getString("numberReport");
                        long dropDataNum = jsonObj.getLongValue("dropDataNum");
                        JSONArray data = jsonObj.getJSONArray("data");

                        StringBuffer sb = new StringBuffer();
                        for (int i = 0; i < explode; i++) {
                            JSONObject json = new JSONObject();
                            // 基本参数
                            json.put("clientId", clientId);
                            json.put("interfaceRecTime", interfaceRecTime);
                            json.put("type", type);
                            json.put("userName", userName);
                            json.put("numberReport", numberReport);
                            json.put("dropDataNum", dropDataNum);
                            json.put("pt", System.currentTimeMillis());
                            JSONArray newData = new JSONArray();
                            String personIdCard, personName;
                            for (int j = 0; j < data.size(); j++) {
                                JSONObject templete = data.getJSONObject(j);
                                JSONObject tmp = JSONObject.parseObject(templete.toJSONString());
                                personIdCard = AESUtil.decrypt(tmp.getString("personIdCard"));
                                personName = AESUtil.decrypt(tmp.getString("personName"));
                                String number = _formatNum(i, 4);
                                personIdCard = sb.append(number).append(personIdCard.substring(4)).toString();
                                sb.delete(0, sb.length());
                                personName = sb.append(personName).append(CONCAT_STR).append(number).toString();
                                sb.delete(0, sb.length());
                                tmp.put("personIdCard", AESUtil.encrypt(personIdCard));
                                tmp.put("personName", AESUtil.encrypt(personName));
                                tmp.put("id", SnowflakeIdWorker.generateId());
                                newData.add(tmp);
                            }
                            json.put("data", newData);
                            collector.collect(json.toJSONString());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        Table resultTable = tEnv.fromDataStream(resultStream, $("pt").proctime());
//        tEnv.createTemporaryView("resultTable", resultTable);
//        tEnv.sqlQuery("select count(0) from resultTable group by TUMBLE(pt, INTERVAL '10' SECOND)").execute().print();

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "NUC_DATA_ZJW", new SimpleStringSchema(), properties, Optional.of(new FlinkVariablePartitioner<>()));
        resultStream.addSink(kafkaSink);

        env.execute("huquan_ProductMsg");
    }


    private static String _formatNum(int number, int lenth) {
        String s = String.valueOf(number);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < lenth - s.length(); i++) {
            sb.append(ZERO);
        }
        return sb.append(s).toString();
    }
}
