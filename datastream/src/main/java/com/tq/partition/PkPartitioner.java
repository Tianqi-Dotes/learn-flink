package com.tq.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class PkPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {

        if (key.equals("tq.com")){
            return 0;
        }else if(key.equals("a.com")){
            return 1;
        }else {
            return 2;
        }
    }
}
