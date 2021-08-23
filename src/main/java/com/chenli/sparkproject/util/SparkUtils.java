package com.chenli.sparkproject.util;

import com.alibaba.fastjson.JSONObject;
import com.chenli.sparkproject.conf.ConfigurationManager;
import com.chenli.sparkproject.constant.Constants;
import com.chenli.sparkproject.test.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * spark工具类
 * @author: 小LeetCode~
 **/
public class SparkUtils {

    /**
     * 根据当前是否本地测试的配置
     * 决定如何设置SparkConf的master
     * @param conf
     */
    public static void setMaster(SparkConf conf) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            conf.setMaster("local");
        }
    }

    /**
     * 生成模拟数据
     * 如果sparl.local配置为true，则生成模拟数据，否则不生成
     * @param sc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext sc,
                            SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc,sqlContext);
        }
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return SQLContext
     */
    public static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 获取指定范围内的用户访问行为数据
     *
     * @param sqlContext
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    public static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext, JSONObject taskParam
    ) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        /**
         * 这里可能会发生Spark SQL并行度不够的问题
         * 比如说，Spark SQL默认就给第一个stage设置了20个task，但是根据你的数据量以及算法时间复杂度
         * 实际上，你需要1000个task去并行执行
         *
         * 所以，这里可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
         */

        //return actionDF.javaRDD().repartition(1000);

        return actionDF.javaRDD();
    }
}
