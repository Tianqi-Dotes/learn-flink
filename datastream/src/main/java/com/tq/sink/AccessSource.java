package com.tq.sink;

import com.tq.transformation.Access;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AccessSource implements SourceFunction<Access> {

    boolean running=true;

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {

        String[] websites={"tq.com","a.com","b.com"};
        Random random = new Random();

        while (running) {
            //每个线程跑10个对象
            for (int i = 0; i < 10; i++) {
                Access access = new Access();
                access.setConsume(random.nextInt(2000));
                access.setDate(123456L);
                access.setWebsite(websites[random.nextInt(websites.length)]);


                ctx.collect(access);
            }
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
