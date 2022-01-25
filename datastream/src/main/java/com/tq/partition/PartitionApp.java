package com.tq.partition;

import com.tq.model.Access;
import com.tq.model.Student;
import com.tq.sink.AccessSource;
import com.tq.sink.AccessSourceMulti;
import com.tq.sink.StudentSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Properties;

public class PartitionApp {

    public static void main(String[] args) throws Exception {
        //创建上下文
        StreamExecutionEnvironment en=StreamExecutionEnvironment.getExecutionEnvironment();
        en.setParallelism(3);

        pa01(en);
        en.execute("DataStreamTest");
    }


    //connect 不同类型 共享状态
    public static void pa01(StreamExecutionEnvironment en){

        DataStreamSource<Access> source1 = en.addSource(new AccessSource());
        System.out.println(source1.getParallelism());
        source1.map(new MapFunction<Access, Tuple2<String,Access>>() {
            @Override
            public Tuple2 map(Access value) throws Exception {
                return Tuple2.of(value.getWebsite(), value);
            }
        })
                .partitionCustom(new PkPartitioner(), new KeySelector<Tuple2<String, Access>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Access> value) throws Exception {
                        return value.f1.getWebsite();
                    }
                })
                //.partitionCustom(new PkPartitioner(),0)//field为字段序列  进行分组
                .map(new MapFunction<Tuple2<String,Access>, Access>() {

                    @Override
                    public Access map(Tuple2<String, Access> value) throws Exception {
                        System.out.println("current thread is " + Thread.currentThread().getId() + "----object:" + value.f1);
                        return value.f1;
                    }
        }).print();
    }

}
