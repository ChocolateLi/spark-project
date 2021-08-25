package com.chenli.sparkproject.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * 组内拼接去重函数（group_concat_distinct()）
 * 技术点4：自定义UDAF聚合函数
 * @author: 小LeetCode~
 **/
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {


    private static final long serialVersionUID = 3709687722916921741L;

    //指定输入数据的字段与类型
    private StructType inputSchema = DataTypes.createStructType(
            Arrays.asList(DataTypes.createStructField("cityInfo", DataTypes.StringType,true)));
    //指定缓冲数据的字段与类型
    private StructType bufferSchema = DataTypes.createStructType(
            Arrays.asList(DataTypes.createStructField("bufferCityInfo", DataTypes.StringType,true)));
    //指定返回类型
    private DataType dataType = DataTypes.StringType;
    //指定是否是正确性的
    private boolean deterministic = true;


    @Override
    public StructType inputSchema() {
        return null;
    }

    @Override
    public StructType bufferSchema() {
        return null;
    }

    @Override
    public DataType dataType() {
        return null;
    }

    @Override
    public boolean deterministic() {
        return false;
    }

    /**
     * 初始化缓冲区
     * 可以认为是自己在内部指定一个初始的值
     * @param buffer
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,"");
    }

    /**
     * 更新
     * 给聚合函数传入一条新数据进行处理
     * 可以理解为，一个个地将组内的字段值传递进来进行处理
     * @param buffer
     * @param input
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        //缓冲区中已经拼接过的城市信息串
        String bufferCityInfo = buffer.getString(0);
        //刚刚传递进来的某个城市的信息
        String cityInfo = input.getString(0);

        //在这里实现去重逻辑
        //判断之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
        if (!bufferCityInfo.contains(cityInfo)) {
            if ("".equals(bufferCityInfo)) {
                bufferCityInfo += cityInfo;
            }else{
                //比如    1:北京
                //1:北京,2:上海
                bufferCityInfo += "," + cityInfo;
            }
            buffer.update(0,bufferCityInfo);
        }
    }

    /**
     * 合并聚合函数的缓冲区
     * update操作，可能针对一个分组内的部分数据，在某个节点上发生的
     * 但是可能一个分组内的数据，会分布在多个节点上处理
     * 此时就要用merge操作，将各个节点上分布式拼接好的串合并起来
     * @param buffer1
     * @param buffer2
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);

        for (String cityInfo:bufferCityInfo2.split(",")){
            if (!bufferCityInfo1.contains(cityInfo)) {
                if ("".equals(bufferCityInfo1)) {
                    bufferCityInfo1 += cityInfo;
                }else{
                    bufferCityInfo1 += "," + cityInfo;
                }
            }
        }

        buffer1.update(0,bufferCityInfo1);
    }

    /**
     * 返回最终结果
     * @param row
     * @return
     */
    @Override
    public Object evaluate(Row row) {
        return row.getString(0);
    }
}
