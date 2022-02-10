package com.tq.flink.domian;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Access {

    public String device;
    public String deviceType;
    public String os;
    public String event;
    public String net;
    public String channel;
    public String uid;
    public Integer nu;  // 1新
    public Integer nu2;
    public String ip;  // ==> ip去解析
    public Long time;
    public String version;

    public String province;
    public String city;

    public Product product;

    public Access setProvince(String pro){
        this.province=pro;
        return this;
    }

}
