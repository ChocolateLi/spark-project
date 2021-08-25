package com.chenli.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

/**
 * 自定义UDF函数获取json的指定数据
 * @author: 小LeetCode~
 **/
public class GetJsonObjectUDF implements UDF2<String,String,String> {

    private static final long serialVersionUID = 1187617733166666465L;

    @Override
    public String call(String json, String field) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(json);
            return jsonObject.getString(field);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
