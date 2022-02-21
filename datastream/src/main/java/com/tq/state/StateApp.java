package com.tq.state;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class StateApp {


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
        en.execute("StateApp");
    }


    /**
     * data :timestamp,string,count
     * @param en
     */
    public static void test01(StreamExecutionEnvironment en){

        List<Tuple2<Long, Long>> source = new ArrayList<>();

        source.add(Tuple2.of(1L,3L));
        source.add(Tuple2.of(1L,7L));
        source.add(Tuple2.of(2L,4L));
        source.add(Tuple2.of(1L,5L));
        source.add(Tuple2.of(2L,2L));
        source.add(Tuple2.of(2L,5L));


        DataStreamSource<Tuple2<Long, Long>> input = en.fromCollection(source);

        input.keyBy(x->x.f0).flatMap(new AvgState()).print();
    }

}
class AvgState extends RichFlatMapFunction<Tuple2<Long, Long>,Tuple2<Long, Double>>{

    private transient ValueState<Tuple2<Long, Long>> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Tuple2<Long, Long>> descriptor=new ValueStateDescriptor<>("avg", Types.TUPLE(Types.LONG,Types.LONG));
        state=getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {

        Tuple2<Long, Long> current = state.value();

        //对当前元素进行状态处理
        if(current==null){
            current=Tuple2.of(0L,0L);
        }
        current.f1=current.f1+value.f1;
        current.f0=current.f0+1;

        state.update(current);

        //每3个数 进行平均数计算并输出
        if(current.f0>=3) {
            out.collect(Tuple2.of(value.f0,current.f1.doubleValue()/current.f0));
            //计算后清零
            state.clear();

        }

    }

}

