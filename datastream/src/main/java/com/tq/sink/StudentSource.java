package com.tq.sink;

import com.tq.model.Student;
import com.tq.utils.MysqlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class StudentSource extends RichSourceFunction<Student> {


    Connection connection;
    PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection= MysqlUtils.getMysqlConnection();
        ps = connection.prepareStatement("select * from  student");
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {

        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            int id = resultSet.getInt("id");
            int age = resultSet.getInt("age");
            String name = resultSet.getString("name");
            Student student = new Student(id, name, age);
            ctx.collect(student);

        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        MysqlUtils.close(connection,ps);
    }

    @Override
    public void cancel() {

    }
}
