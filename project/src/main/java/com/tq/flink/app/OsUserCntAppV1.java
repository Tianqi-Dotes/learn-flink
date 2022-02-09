package com.tq.flink.app;

import com.alibaba.fastjson.JSON;
import com.tq.flink.domian.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 按照操作系统维度   新老用户分析
 */
public class OsUserCntAppV1 {

    private static FlinkJedisPoolConfig conf;

    static {
        conf = new FlinkJedisPoolConfig.Builder().setHost("129.211.125.29").setPort(4188).setPassword("#edcVFR4").build();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<Access> startupStream = en.readTextFile("logs/access.json").map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {

                try {

                    return JSON.parseObject(value, Access.class);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).filter(x -> x != null)//清洗异常数据
                .filter(x -> x.getEvent().equals("startup"));//过滤 开机动作 数据

        //startupStream.print();

        //tuple格式 os 新老用户 计数1
        //SingleOutputStreamOperator f2 = processOsUser(startupStream);


        //根据新老用户统计
        SingleOutputStreamOperator f2 = processNewUser(startupStream);


        //写入redis
        //f2.addSink(new RedisSink<Tuple3<String, Integer, Integer>>(conf, new OsUserRedisMapper()));

        //写入redis
        f2.addSink(new RedisSink<Tuple2<Integer, Integer>>(conf, new NewUserRedisMapper()));

        f2.print();
        en.execute("OsUserCntAppV1");
    }

    //按照os 新老用户维度区分
    private static SingleOutputStreamOperator processOsUser(SingleOutputStreamOperator<Access> startupStream){
        //tuple格式 os 新老用户 计数1
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> processStream = startupStream.map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                return Tuple3.of(value.getOs(), value.getNu(), 1);
            }
        });

        // tuple格式 os 新老用户 计数1 -》对os 新老用户进行keyby 再去sum1
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> f2 = processStream
                .keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                }).sum(2)
                .setParallelism(1);//但并行度 保证顺序
        //(Android,1,29) (Android,0,17)
        //(iOS,1,38) (iOS,0,16)

        return f2;
    }

    //按照新老用户维度区分
    private static SingleOutputStreamOperator processNewUser(SingleOutputStreamOperator<Access> startupStream){
        //tuple格式  新老用户 计数1
        SingleOutputStreamOperator<Tuple2< Integer, Integer>> processStream = startupStream.map(new MapFunction<Access, Tuple2< Integer, Integer>>() {
            @Override
            public Tuple2< Integer, Integer> map(Access value) throws Exception {
                return Tuple2.of( value.getNu(), 1);
            }
        });

        // tuple格式 os 新老用户 计数1 -》对os 新老用户进行keyby 再去sum1
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> f2 = processStream
                .keyBy(new KeySelector<Tuple2<Integer, Integer>, Tuple1<Integer>>() {
                    @Override
                    public Tuple1<Integer> getKey(Tuple2<Integer, Integer> value) throws Exception {
                        return Tuple1.of(value.f0);
                    }
                }).sum(1)
                .setParallelism(1);//但并行度 保证顺序
//(Android,1,29) (Android,0,17)
        //(iOS,1,38) (iOS,0,16)

        return f2;
    }
}

class NewUserRedisMapper implements RedisMapper<Tuple2<Integer, Integer>> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "tq_newuser_v1");
    }

    @Override
    public String getKeyFromData(Tuple2<Integer, Integer> data) {
        return data.f0+"";
    }

    @Override
    public String getValueFromData(Tuple2<Integer, Integer> data) {
        return data.f1+"";
    }

}



class OsUserRedisMapper implements RedisMapper<Tuple3<String, Integer, Integer>> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "tq_osuser_v1");
    }

    @Override
    public String getKeyFromData(Tuple3<String, Integer, Integer> data) {
        return data.f0+data.f1;
    }

    @Override
    public String getValueFromData(Tuple3<String, Integer, Integer> data) {
        return data.f2.toString();
    }

}
