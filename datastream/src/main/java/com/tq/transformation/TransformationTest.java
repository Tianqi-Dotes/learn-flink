package com.tq.transformation;

import com.tq.model.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TransformationTest {

    public static void main(String[] args) throws Exception {
        //创建上下文
        StreamExecutionEnvironment en=StreamExecutionEnvironment.getExecutionEnvironment();
        en.setParallelism(3);
        //reduce(en);
        richMap(en);
        en.execute("TransformationTest");
    }


    public static void richMap(StreamExecutionEnvironment en) {
        DataStreamSource<String> source = en.readTextFile("logs/access.log");
        SingleOutputStreamOperator<Access> map = source.map(new AccessMapFunction());
        map.print();
    }

    /**
     * 读入数据
     * 拆分
     * 为每个单词 计数
     * 先按单词keyby  再求和
     * @param en
     */
    public static void reduce(StreamExecutionEnvironment en) {
        //pk,pk,flink pk,pk,spark

        DataStreamSource<String> localhost = en.socketTextStream("localhost", 9527);
        SingleOutputStreamOperator<String> out = localhost.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
                return;
            }
        });

        out.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String value) throws Exception {
                return new Tuple2<>(value,1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {//tuple2 f0相同的 key 进入一起 v1 v2
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1) ;
            }
        }).print();
    }


    /**
     * key by 操作 按照网站分组求和
     * @param en
     */
    public static void keyBy(StreamExecutionEnvironment en) {

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
       /* KeyedStream<Access, Tuple> website = map.keyBy("website");
        website.sum("consume").print();*/
        map.keyBy(Access::getWebsite).sum("consume").print();
    }

    /**
     * flat map操作
     * @param en
     */
    public static void flatMap(StreamExecutionEnvironment en) {
        //pk,pk,flink pk,pk,spark

        DataStreamSource<String> localhost = en.socketTextStream("localhost", 9527);
        SingleOutputStreamOperator<String> out = localhost.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
                return;
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.equals("pk");
            }
        });
        out.print();
    }

    /**
     * data stream  过滤操作
     * @param en
     */
    public static void filter(StreamExecutionEnvironment en) {
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
        SingleOutputStreamOperator<Access> filter = map.filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access value) throws Exception {
                return value.getConsume() > 4000;
            }
        });
        filter.print();
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
