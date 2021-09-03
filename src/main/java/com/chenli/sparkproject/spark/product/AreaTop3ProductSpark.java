package com.chenli.sparkproject.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.chenli.sparkproject.conf.ConfigurationManager;
import com.chenli.sparkproject.constant.Constants;
import com.chenli.sparkproject.dao.IAreaTop3ProductDAO;
import com.chenli.sparkproject.dao.ITaskDAO;
import com.chenli.sparkproject.domain.AreaTop3Product;
import com.chenli.sparkproject.domain.Task;
import com.chenli.sparkproject.factory.DAOFactory;
import com.chenli.sparkproject.util.ParamUtils;
import com.chenli.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 各区域top3热门商品统计Spark作业
 * @author: 小LeetCode~
 **/
@SuppressWarnings("all")
public class AreaTop3ProductSpark {

    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("AreaTop3ProductSpark");
        SparkUtils.setMaster(conf);

        // 构建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //注册自定义函数
        sqlContext.udf().register("concat_long_string",
                new ConcatLongStringUDF(),DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct",
                new GroupConcatDistinctUDAF());
        sqlContext.udf().register("get_json_object",
                new GetJsonObjectUDF(),DataTypes.StringType);


        // 准备模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 获取命令行传入的taskid，查询对应的任务参数
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        long taskid = ParamUtils.getTaskIdFromArgs(args,
                Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Task task = taskDAO.findById(taskid);

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        //查询用户指定日期范围内的点击行为  <cityid,Row>
        //技术点1：Hive数据源的使用
        JavaPairRDD<Long,Row> cityid2clickActionRDd = getClickActionRDDByDate(
                sqlContext,startDate,endDate);

        //从MySQL中查询城市信息 <cityid,Row>
        //技术点2：异构数据源之Mysql的使用
        JavaPairRDD<Long,Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);

        //生成点击商品基础信息临时表
        //技术点3：将RDD转换为DataFrame，并注册临时表
        generateTempClickProductBasicTable(sqlContext,
                cityid2clickActionRDd, cityid2cityInfoRDD);

        //生成各区域各商品点击次数的临时表
        generateTempAreaProductClickCountTable(sqlContext);

        //生成区域商品点击次数临时表（包含了商品的完整信息）
        generateTempAreaFullProductClickCountTable(sqlContext);

        //使用开窗函数获取各个区域内点击次数排名前3的热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);

        // 这边的写入mysql和之前不太一样
        // 因为实际上，就这个业务需求而言，计算出来的最终数据量是比较小的
        // 总共就不到10个区域，每个区域还是top3热门商品，总共最后数据量也就是几十个
        // 所以可以直接将数据collect()到本地
        // 用批量插入的方式，一次性插入mysql即可
        List<Row> rows = areaTop3ProductRDD.collect();
        persistAreaTop3Product(taskid, rows);

        sc.close();
    }




    /**
     * 查询指定日期范围内的点击行为数据
     * @param sqlContext
     * @param startDate 起始日期
     * @param endDate   截止日期
     * @return  点击行为数据
     */
    private static JavaPairRDD<Long, Row> getClickActionRDDByDate(
            SQLContext sqlContext, String startDate, String endDate) {
        // 从user_visit_action中，查询用户访问行为数据
        // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
        // 第二个限定：在用户指定的日期范围内的数据

        String sql =
                "SELECT "
                        + "city_id,"
                        + "click_product_id product_id "
                        + "FROM user_visit_action "
                        + "WHERE click_product_id IS NOT NULL "
                        + "AND click_product_id != 'NULL' "
                        + "AND click_product_id != 'null' "
                        + "AND action_time>='" + startDate + "' "
                        + "AND action_time<='" + endDate + "'";

        DataFrame clickActionDF = sqlContext.sql(sql);

        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Long cityid = row.getLong(0);
                        return new Tuple2<>(cityid,row);
                    }
                }
        );

        return cityid2clickActionRDD;
    }

    /**
     * 使用Spark sql从Mysql中查询城市信息
     * @param sqlContext
     * @return
     */
    private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
        // 构建Mysql连接配置信息（直接从配置文件中获取）
        String url = null;
        String user = null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if(local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String,String> options = new HashMap<>();
        options.put("url",url);
        options.put("dbtable", "city_info");

        //通过SqlContext从Mysql中查询数据
        DataFrame cityInfoDF = sqlContext.read().format("jdbc")
                .options(options).load();

        //返回RDD
        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        //long cityid = row.getLong(0);
                        long cityid = Long.valueOf(String.valueOf(row.get(0)));
                        return new Tuple2<Long,Row>(cityid, row);
                    }
                }
        );

        return cityid2cityInfoRDD;

    }


    /**
     * 生成点击商品基础信息临时表
     * @param sqlContext
     * @param cityid2clickActionRDd
     * @param cityid2cityInfoRDD
     */
    private static void generateTempClickProductBasicTable(
            SQLContext sqlContext,
            JavaPairRDD<Long, Row> cityid2clickActionRDd,
            JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
        //执行join操作，进行点击行为数据和城市数据的关系
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD =
                cityid2clickActionRDd.join(cityid2cityInfoRDD);

        //将上面的JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DateFrame）
        JavaRDD<Row> mappedRDD = joinedRDD.map(
                new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
                    @Override
                    public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
                        long cityid = tuple._1;
                        Row clickAction = tuple._2._1;
                        Row cityInfo = tuple._2._2;

                        long productid = clickAction.getLong(1);
                        String cityName = cityInfo.getString(1);
                        String area = cityInfo.getString(2);

                        return RowFactory.create(cityid,cityName,area,productid);
                    }
                }
        );

        //基于JavaRDD<Row> 的格式，就可以将其转换为DateFrame
        //定义行结构字段信息
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
        //创建用StructType来表示的行结构信息
        StructType schema = DataTypes.createStructType(structFields);

        DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);

        //将DataFrame中的数据，注册成临时表(tmp_click_product_basic)
        df.registerTempTable("tmp_click_product_basic");
    }

    /**
     * 生成各区域各商品点击次数临时表
     * @param sqlContext
     */
    private static void generateTempAreaProductClickCountTable(
            SQLContext sqlContext) {
        //按照area和product_id两个字段进行分组
        //计算出各区域各商品的点击次数
        //可以获取到每个area下的每个product_id的城市信息拼接起来的串
        String sql =
                "select"
                    + "area,"
                    + "product_id,"
                    + "count(*) click_count,"
                    + "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "
                    + "from tmp_click_product_basic "
                    + "group by area,product_id ";

        //使用spark sql 执行这条SQL语句
        DataFrame df = sqlContext.sql(sql);

        //再次将查询出来的数据注册为一个临时表
        //各区域各商品的点击次数
        df.registerTempTable("tmp_are_product_click_count");

    }

    /**
     * 生成区域商品点击次数临时表（包含了商品的完整信息）
     * @param sqlContext
     */
    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
        // 将之前得到的各区域各商品点击次数表，product_id
        // 去关联商品信息表，product_id，product_name和product_status
        // product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
        // get_json_object()函数，可以从json串中获取指定的字段的值
        // if()函数，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
        // area, product_id, click_count, city_infos, product_name, product_status

        // 为什么要费时费力，计算出来商品经营类型
        // 你拿到到了某个区域top3热门的商品，那么其实这个商品是自营的，还是第三方的
        // 其实是很重要的一件事

        //内置if()函数的使用

        String sql =
                "select "
                    + "tapcc.area,"
                    + "tapcc.product_id,"
                    + "tapcc.click_count,"
                    + "pi.product_name,"
                    + "if(get_json_object(pi.extend_info,'product_status')='0','自营商品','第三方商品') product_status"
                + "from tmp_area_product_click_count tapcc "
                + "join product_info pi on tapcc.product_id = pi.product_id ";

        DataFrame df = sqlContext.sql(sql);

        df.registerTempTable("tmp_area_fullprod_click_count");
    }


    /**
     * 获取各区域top3热门商品
     * @param sqlContext
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
        //技术点：使用开窗函数

        //使用开创函数先进行一个子查询
        //按转area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
        //接着在外层查询中，过滤各个组内的行号排名前3的数据
        //其实就是咱们的各个区域下top3热门商品
        String sql =
                "select "
                    + "area,"
                    + "case "
                        + "when area='华北' or area='华东' then 'A级' "
                        + "when area='华南' or area='华中' then 'B级' "
                        + "when area='西北' or area='西南' then 'C级' "
                        + "else 'D级'"
                    + "end area_level,"
                    + "product_id,"
                    + "click_count,"
                    + "product_name,"
                    + "product_status"
                + "from("
                    + "select "
                        + "area,"
                        + "product_id,"
                        + "click_count,"
                        + "product_name,"
                        + "product_status"
                        + "row_number() over (partition by area order by click_count desc) rank"
                + ") t"
                + "where rank<=3";

        DataFrame df = sqlContext.sql(sql);

        return df.javaRDD();
    }

    /**
     * 将计算出来的各区域top3热门商品写入Mysql中
     * @param taskid
     * @param rows
     */
    private static void persistAreaTop3Product(long taskid, List<Row> rows) {
        List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();

        for(Row row : rows) {
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskid);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));
            areaTop3Products.add(areaTop3Product);
        }

        IAreaTop3ProductDAO areTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        areTop3ProductDAO.insertBatch(areaTop3Products);

    }


}
