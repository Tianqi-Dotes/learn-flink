package com.tq.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowApp {


    /**
     * 重点1窗口：
     * TumblingWindow 按时间分配 无overlap
     * SlidingWindow 按时间分配 有overlap 参数：窗口大小，滑动时间
     * SessionWindow 设定时间间隔 如果时间间隔内没有数据 则下一个窗口
     * GlobalWindow 不用
     *
     * 2Window Function 分为增量和 全量
     * 增量：reduce agg
     * 全量：process
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en = StreamExecutionEnvironment.getExecutionEnvironment();


        //test01(en);
        //test02(en);
        //test03(en);
        test04(en);
        en.execute("WindowApp");
    }


    /**
     * note: ProcessingTime  系统时间，与数据本身的时间戳无关，即在window窗口内计算完成的时间 高吞吐 低延时
     * EventTime 事件产生的时间，即数据产生时自带时间戳 可处理乱序 效率低
     * 5秒一个窗口 求和
     * @param en
     */
    public static void test01(StreamExecutionEnvironment en){
        DataStreamSource<String> input = en.socketTextStream("localhost", 9527);
        //en.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        input.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        }).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //.timeWindowAll(Time.seconds(5))//5秒一个窗口
                .sum(0).print();
    }

    /**
     * 5秒一个窗口 keyby求和
     *
     * data:hadoop,1
     * flink,1
     * @param en
     */
    public static void test02(StreamExecutionEnvironment en){
        DataStreamSource<String> input = en.socketTextStream("localhost", 9527);
        //en.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        input.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");

                return Tuple2.of(split[0],Integer.valueOf(split[1]));
            }
        }).keyBy(key->key.f0)
    /*.keyBy(new KeySelector<Tuple2<String, Integer>, Tuple1<String>>() {
            @Override
            public Tuple1<String> getKey(Tuple2<String, Integer> value) throws Exception {
                return Tuple1.of(value.f0);
            }})*/
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //.timeWindowAll(Time.seconds(5))//5秒一个窗口
                .sum(1).print();
    }


    /**
     * 5秒一个窗口 keyby求和  reduce重写sum
     *window function 增量处理
     * data:hadoop,1
     * flink,1
     * @param en
     */
    public static void test03(StreamExecutionEnvironment en){
        DataStreamSource<String> input = en.socketTextStream("localhost", 9527);
        //en.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        input.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");

                return Tuple2.of(split[0],Integer.valueOf(split[1]));
            }
        }).keyBy(key->key.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //.timeWindowAll(Time.seconds(5))//5秒一个窗口
                //.sum(1)

                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {//两条数据才会走reduce
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                })
                .print();
    }

    /**
     * processed time window
     * @param en
     */
    public static void test04(StreamExecutionEnvironment en){
        DataStreamSource<String> input = en.socketTextStream("localhost", 9527);
        //en.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        input.map(new MapFunction<String, Tuple2<String,Integer>>() {//只敲数字
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {

                return Tuple2.of("tq", Integer.valueOf(value));
            }
        }).keyBy(key->key.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new MyProcessedWindow())//窗口功能：取最大
                .print();
    }
}
