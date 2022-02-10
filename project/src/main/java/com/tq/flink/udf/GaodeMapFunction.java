package com.tq.flink.udf;

import com.alibaba.fastjson.JSONObject;
import com.tq.flink.domian.Access;
import com.tq.flink.utils.MapUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;
import java.util.Map;

public class GaodeMapFunction extends RichMapFunction<Access,Access> {

    static final String GAODE_IP_URL="https://restapi.amap.com/v5/ip"+"?";
    CloseableHttpClient httpClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        //如果使用httpclient 需要初始化httpclient
        httpClient = HttpClients.createDefault();

    }

    @Override
    public void close() throws Exception {
        //如果使用httpclient 需要关闭httpclient
    }

    @Override
    public Access map(Access value) throws Exception {
        Map<String,Object> params=new HashMap<>();
        params.put("key","63b0b3fd4c98f8ae83e8fda1b7865ecb");
        params.put("type","4");
        params.put("ip",value.getIp());

        StringBuilder sb = new StringBuilder(GAODE_IP_URL);
        params.keySet().forEach(k -> {

            Object v = params.get(k);
            sb.append("&");
            sb.append(k);
            sb.append("=");
            sb.append(v.toString());
        });
        String requestUrl = sb.toString();

        HttpGet httpGet = new HttpGet(requestUrl);
        CloseableHttpResponse response = httpClient.execute(httpGet);

        String res = EntityUtils.toString(response.getEntity(), "utf8");
        JSONObject parse = (JSONObject) JSONObject.parse(res);
        String province = parse.get("province").toString();


        if (province!=null&&!province.equals("")){
            value.setProvince(province);
        }
        return value;
    }
}
