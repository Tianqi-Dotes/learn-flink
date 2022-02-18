package com.tq.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * IN:输入DATASTREAM里的类型
 * OUT:输出DATASTREAM的类型
 * KEY:KEYBY的类型
 * W： 时间窗口
 */
public class MyProcessedWindow extends ProcessWindowFunction<Tuple2<String,Integer>,String,String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

        System.out.println("process invoked");
        int max=Integer.MIN_VALUE;
        for (Tuple2<String, Integer> element : elements) {
            max = Math.max(max, element.f1);
        }
        out.collect("全量最大数为："+max);
    }
}
