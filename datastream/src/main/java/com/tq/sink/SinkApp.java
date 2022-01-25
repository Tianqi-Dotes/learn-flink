package com.tq.sink;

import com.tq.model.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en = StreamExecutionEnvironment.getExecutionEnvironment();
        //DataStreamSource<String> source = en.socketTextStream("localhost", 9527);

        //System.out.println("source param: "+source.getParallelism());
        //source.printToErr().setParallelism(2);
        //source.print("test").setParallelism(2);//并行度为1  没有 identifier

        sink(en);
        en.execute("SinkApp");
    }


    /**
     * 生产后sink
     * @param en
     */
    public static void sink(StreamExecutionEnvironment en) {

        //access.log line data->object
        DataStreamSource<String> source = en.readTextFile("logs/access.log");
        SingleOutputStreamOperator<Access> map = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] split = value.split(",");
                Access access = new Access();
                access.setDate(Long.parseLong(split[0]));
                access.setWebsite(split[1]);
                access.setConsume(Integer.valueOf(split[2]));
                return access;
            }
        });

        SingleOutputStreamOperator<Access> res = map
                .keyBy(Access::getWebsite).sum("consume");
        res.print();
        res.map(new MapFunction<Access, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Access value) throws Exception {

                return Tuple2.of(value.getWebsite(),value.getConsume());
            }
        }).addSink(new MysqlSink());
    }

}
