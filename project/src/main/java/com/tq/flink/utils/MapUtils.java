package com.tq.flink.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;*/

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapUtils {
    private static Logger LOGGER = LoggerFactory.getLogger(MapUtils.class);

    static final String GAODE_IP_URL="https://restapi.amap.com/v5/ip"+"?";


    /*public static String getIpProvinceFromGaoDe(String ip) {
        Map<String,Object> params=new HashMap<>();
        params.put("key","63b0b3fd4c98f8ae83e8fda1b7865ecb");
        params.put("type","4");
        params.put("ip",ip);


        String res = getRequestForJson(params, GAODE_IP_URL);

        //LOGGER.info(res);
        JSONObject parse = (JSONObject)JSONObject.parse(res);
        String province = parse.get("province").toString();

        return province;
    }*/


   /* public static String postRequestForJson(Object jsonBody,String url) {

        RestTemplate restTemplate=new RestTemplate();
        HttpHeaders headers = new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON);
        String req= JSON.toJSON(jsonBody).toString();
        HttpEntity<String> entity = new HttpEntity<String>(req,headers);
        ResponseEntity<String> res= restTemplate.postForEntity(url,entity,String.class);
        //List<Map<String,Object>> list2= (List<Map<String, Object>>) JSON.parse(res.getBody());


        return res.getBody();
    }

    public static String getRequestForJson(Map<String,Object> params,String url) {


        StringBuilder sb = new StringBuilder(url);
        params.keySet().forEach(k -> {

            Object value = params.get(k);
            sb.append("&");
            sb.append(k);
            sb.append("=");
            sb.append(value.toString());
        });
        String requestUrl = sb.toString();

        RestTemplate restTemplate = new RestTemplate();

        HttpHeaders headers = new HttpHeaders();
        HttpEntity<Object> requestEntity = new HttpEntity<>(null, headers);
        try {
            ResponseEntity<String> res = restTemplate.exchange(requestUrl, HttpMethod.GET, requestEntity, String.class);

            return res.getBody();
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }*/

}
