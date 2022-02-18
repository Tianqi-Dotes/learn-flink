package com.tq.watermark;

import com.tq.window.MyProcessedWindow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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

        test01(en);
        en.execute("WatermarkApp");
    }


    /**
     * data :timestamp,string,count
     * @param en
     */
    public static void test01(StreamExecutionEnvironment en){
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
                .sum(1).print();
    }

}
