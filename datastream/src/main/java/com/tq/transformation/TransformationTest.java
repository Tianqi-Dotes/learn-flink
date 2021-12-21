package com.tq.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class TransformationTest {

    public static void main(String[] args) throws Exception {
        //创建上下文
        StreamExecutionEnvironment en=StreamExecutionEnvironment.getExecutionEnvironment();

        //map(en);
        en.execute("TransformationTest");
    }

    /**
     * line data->entity
     * @param en
     */
    public static void map(StreamExecutionEnvironment en) {

        //access.log line data->object
      /*  DataStreamSource<String> source = en.readTextFile("logs/access.log");
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
        map.print();*/


        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);

        DataStreamSource<Integer> source = en.fromCollection(list);
        SingleOutputStreamOperator<Integer> map = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });
        map.print();
    }
}
