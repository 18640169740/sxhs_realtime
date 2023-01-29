//package com.sxhs.realtime.window;
//
//import com.sxhs.realtime.util.MyHbaseSink;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import scala.Tuple2;
//
//public class TestHbase {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        DataStreamSource<String> stream = env.socketTextStream("localhost", 7777);
//
//        MyHbaseSink hbaseSink = new MyHbaseSink("caches", "nuc_collect_distinct", 10, 1000l);
//        stream.addSink(hbaseSink);
//        env.execute();
//    }
//}
