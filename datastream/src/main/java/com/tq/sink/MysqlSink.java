package com.tq.sink;

import com.tq.utils.MysqlUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * 将access数据写入mysql
 */
public class MysqlSink extends RichSinkFunction<Tuple2<String,Integer>> {

    Connection connection;
    PreparedStatement insertPs;
    PreparedStatement updatePs;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection=MysqlUtils.getMysqlConnection();
        insertPs = connection.prepareStatement("insert into traffic (domain ,consume) values (?,?)");
        updatePs = connection.prepareStatement("update traffic set consume=? where domain=?");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (insertPs!=null){
            insertPs.close();
        }
        if (updatePs!=null){
            updatePs.close();
        }
        if (connection!=null){
            connection.close();
        }

    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        super.invoke(value, context);
        //打印对象
        System.out.println("invoke!!!!!!!!!!!!!!!!object===website:"+value.f0+"===consume:"+value.f1);

        updatePs.setInt(1,value.f1);
        updatePs.setString(2, value.f0);
        updatePs.execute();
        if (updatePs.getUpdateCount()==0){
            insertPs.setString(1, value.f0);
            insertPs.setInt(2,value.f1);
            insertPs.execute();
        }

    }
}
