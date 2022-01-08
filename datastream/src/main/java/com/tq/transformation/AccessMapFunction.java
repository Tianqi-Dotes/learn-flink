package com.tq.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import com.tq.transformation.Access;

public class AccessMapFunction extends RichMapFunction<String,Access> {

    //业务逻辑操作 每条数据执行一次
    @Override
    public Access map(String value) throws Exception {
        System.out.println("maping~~~~~~~~~~~~");

        String[] split = value.split(",");
        Access access = new Access();
        access.setDate(Long.parseLong(split[0]));
        access.setWebsite(split[1]);
        access.setConsume(Integer.valueOf(split[2]));
        return access;

    }

    //生命周期方法
    //初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("opening~~~~~~~~~~~~");

    }

    /**
     * 清理  关掉connect
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("closing~~~~~~~~~~~~");
        super.close();
    }


    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

}
