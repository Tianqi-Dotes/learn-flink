package com.tq.flink.udf;

import com.alibaba.fastjson.JSONObject;
import com.tq.flink.domian.Access;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class GaodeMapAsyncFunction extends RichAsyncFunction<Access,Access> {

    static final String GAODE_IP_URL="https://restapi.amap.com/v5/ip"+"?";
    CloseableHttpAsyncClient httpClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        //如果使用httpclient 需要初始化httpclient
        httpClient = HttpAsyncClients.createDefault();
    }

    @Override
    public void close() throws Exception {
        //如果使用httpclient 需要关闭httpclient
        httpClient.close();
    }

    public Future<HttpResponse> getNewAccess(Access value) throws Exception {
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
        Future<HttpResponse> response = httpClient.execute(httpGet, null);


        return response;
        /*String res = EntityUtils.toString(response.get().getEntity(), "utf8");
        JSONObject parse = (JSONObject) JSONObject.parse(res);
        String province = parse.get("province").toString();


        if (province!=null&&!province.equals("")){
            value.setProvince(province);
        }
        return new Future;*/
    }

    @Override
    public void asyncInvoke(Access input, ResultFuture<Access> resultFuture) throws Exception {
        // issue the asynchronous request, receive a future for result
        final Future<HttpResponse> result = getNewAccess(input);

        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        CompletableFuture.supplyAsync(new Supplier<HttpResponse>() {

            @Override
            public HttpResponse get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // Normally handled explicitly.
                    return null;
                }
            }
        }).thenAccept( (HttpResponse res) -> {
            try {
                resultFuture.complete(
                        Collections.singleton(input.setProvince(((JSONObject) JSONObject.parse(EntityUtils.toString(res.getEntity(), "utf8"))).get("province").toString())));
            } catch (IOException e) {
                e.printStackTrace();
            }
            ;
        });
    }
}
