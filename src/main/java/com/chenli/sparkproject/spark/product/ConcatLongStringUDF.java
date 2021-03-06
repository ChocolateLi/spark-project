package com.chenli.sparkproject.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * 将两个字段拼接起来（使用指定的分隔符）
 * 技术点：自定义UDF函数
 * @author: 小LeetCode~
 **/
public class ConcatLongStringUDF implements UDF3<Long,String,String,String> {
    @Override
    public String call(Long v1, String v2, String split) throws Exception {
        return String.valueOf(v1) + split + v2;
    }
}
