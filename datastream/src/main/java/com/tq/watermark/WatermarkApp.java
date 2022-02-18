package com.tq.watermark;

import com.tq.window.MyProcessedWindow;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WatermarkApp {


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

        test02(en);
        en.execute("WatermarkApp");
    }


    /**
     * data :timestamp,string,count
     * @param en
     */
    public static void test01(StreamExecutionEnvironment en){
        DataStreamSource<String> input = en.socketTextStream("localhost", 9527);

        //时间watermark
        //添加延时2
        //watermark=延时时间+窗口时间
        SingleOutputStreamOperator<String> in = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                return Long.valueOf(split[0]);
            }
        });
        in.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[1],Integer.valueOf(split[2]));
            }
        }).keyBy(x->x.f0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1).print();
    }


    /**
     * 重写sum函数 使用windowfunction
     * reduce  增量
     * process 全量
     * @param en
     */
    public static void test02(StreamExecutionEnvironment en){
        DataStreamSource<String> input = en.socketTextStream("localhost", 9527);

        //时间watermark
        SingleOutputStreamOperator<String> in = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                return Long.valueOf(split[0]);
            }
        });
        in.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[1],Integer.valueOf(split[2]));
            }
        }).keyBy(x->x.f0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {

                        System.out.println("input----" + value1.toString() + "----input2---" + value2.toString());

                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    //全量function
                    FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

                        for (Tuple2<String, Integer> element : elements) {
                            System.out.println("[[[start::"+dateFormat.format(context.window().getStart())+"--end::"
                                    +dateFormat.format(context.window().getEnd())+"--data:"+element.toString());
                        }

                    }
                })
                //.sum(1)
                .print();
    }

}
