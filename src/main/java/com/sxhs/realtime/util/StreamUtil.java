package com.sxhs.realtime.util;

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
                                collector.collect(JSONObject.parseObject(value, CollectDataId.class));
                                break;
                            case "TRANSPORT_DATA":
                                ctx.output(transportTag, JSONObject.parseObject(value, TransportDataId.class));
                                break;
                            case "RECEIVE_DATA":
                                ctx.output(receiveTag, JSONObject.parseObject(value, ReceiveDataId.class));
                                break;
                            case "REPORT_DATA":
                            case "XA_REPORT_DATA":
                                ctx.output(reportTag, JSONObject.parseObject(value, ReportDataId.class));
                                break;
                            default:
                                logger.error("unknow type {}", type);
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
}
