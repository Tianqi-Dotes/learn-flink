package com.tq.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = en.socketTextStream("localhost", 9527);

        System.out.println("source param: "+source.getParallelism());
        //source.printToErr().setParallelism(2);
        source.print("test").setParallelism(2);//并行度为1  没有 identifier

        en.execute("SinkApp");
    }



}
