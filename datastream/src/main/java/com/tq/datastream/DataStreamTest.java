package com.tq.datastream;

import com.tq.sink.AccessSource;
import com.tq.sink.AccessSourceMulti;
import com.tq.transformation.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.NumberSequenceIterator;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;

public class DataStreamTest {

    public static void main(String[] args) throws Exception {
        //创建上下文
        StreamExecutionEnvironment en=StreamExecutionEnvironment.getExecutionEnvironment();

        //test03(en);
        //test04(en);
        test05(en);
        en.execute("DataStreamTest");
    }

    //多线程source
    public static void test05(StreamExecutionEnvironment en){

        //source 并行度默认为cpu线程数
        DataStreamSource<Access> accessDataStreamSource = en.addSource(new AccessSourceMulti()).setParallelism(16);
        System.out.println(accessDataStreamSource.getParallelism());
        accessDataStreamSource.print();
    }
    //单线程source
    public static void test04(StreamExecutionEnvironment en){

        //source 并行度默认为1
        DataStreamSource<Access> accessDataStreamSource = en.addSource(new AccessSource());
        System.out.println(accessDataStreamSource.getParallelism());
        accessDataStreamSource.print();
    }

    //接入kafka
    public static void test03(StreamExecutionEnvironment en) {
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group_id","test");
        //接入源
        DataStreamSource<String> kafka = en.addSource(new FlinkKafkaConsumer<>("flink", new SimpleStringSchema(), properties));
        System.out.println(kafka.getParallelism());
        kafka.print();

    }


    public static void test02(StreamExecutionEnvironment en) throws Exception {

        DataStreamSource<Long> source = en.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);

        SingleOutputStreamOperator<Long> filter = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value>5;
            }
        });
        System.out.println(source.getParallelism());//source的并行数

        System.out.println(filter.getParallelism());//filter的并行数  机器核心数
        filter.print();
        en.execute("DataStreamTest");
    }

    public static void test01(StreamExecutionEnvironment en) throws Exception {

        DataStreamSource<String> source = en.socketTextStream("localhost", 9527);

        SingleOutputStreamOperator<String> filter = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.equals("tq");
            }
        }).setParallelism(32);
        System.out.println(source.getParallelism());//source的并行数

        System.out.println(filter.getParallelism());//filter的并行数  机器核心数
        filter.print();
        en.execute("DataStreamTest");
    }
}
