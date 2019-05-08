package com.ltz.spark.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.util.Random;

/**
 * 用户行为类型
 * 搜索、点击、下单及支付
 * @author pengyucheng
 */
enum ActionType
{
    SEARCH,CLICK,ORDER,PAY;
}

/**
 * spark应用程序
 *  本地测试数据用数据生成器
 * @author pengyucheng
 */
public class LocalDataGenerator
{
    /**
     * 模拟生产环境hive表数据
     * 本地测试用
     * @param jsc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext jsc,SQLContext sqlContext)
    {
        mockUserInfo(jsc,sqlContext);
        mockUseSessionInfo(jsc,sqlContext);
    }

    /**
     * 模拟产生hive中 user_info表的数据
     * @param sqlContext
     */
    public static void mockUserInfo(JavaSparkContext jsc ,SQLContext sqlContext)
    {
        /*
         * 1、生成 List<Row> 与 定义RDD中row的每列数据类型 ：完成非结构化数据到结构化数据的转换
         */
        List<Row> rows = new ArrayList<Row>();
        Row row = null;

        long userId = 0;
        String username = null;
        String name = null;
        int age = 0;
        String professional = null;
        String city = null;
        String[] sexStrs = new String[]{"man","woman"};
        String sex = null;
        Random random =  new Random(666);
        for (int i = 0; i < 100; i++)
        {
            userId = random.nextInt(100);
            username = "username"+userId;
            name = "name"+userId;
            age = random.nextInt(100);
            professional = "professional"+userId;
            city = "city"+userId;
            sex = sexStrs[random.nextInt(2)];
            row = RowFactory.create(userId,username,name,age,professional,city,sex);
            rows.add(row);
        }
        JavaRDD<Row> rowRDD = jsc.parallelize(rows);
        StructType st = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, false),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)
        ));

        /*
         * 2、rowRDD 转化成 DataFrame
         */
        DataFrame df = sqlContext.createDataFrame(rowRDD, st);
        df.printSchema();

        /*
         * 3、将内存中的数据，注册临时表
         */
        df.registerTempTable("user_info");
    }

    /**
     * 模拟hive中 user_session_info表数据
     * @param sqlContext
     */
    public static void mockUseSessionInfo(JavaSparkContext jsc ,SQLContext sqlContext)
    {
        List<Row> rows = new ArrayList<Row>();
        Random random = new Random();
        String date = DateUtils.formatDate(new Date());
        String[] searchKeyWords = new String[]{"火锅","蛋糕","重庆辣子鸡","重新小面"
                ,"海底捞","道鱼火锅","国贸大厦","日本料理","温泉","太古商城"};
        for (int i = 0; i < 100; i++)
        {
            long userId = random.nextInt(100);
            for (int j = 0; j < 10; j++)
            {
                String sessionId = UUID.randomUUID().toString();
                for (int k = 0; k < 10; k++)
                {
                    String searchKeyWord = null;
                    Long clickCategoryId = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    long pageid = random.nextInt(10);
                    String actionTime = date + " " + random.nextInt(24)+":"+random.nextInt(60)+":"+random.nextInt(60);
                    ActionType actionType = ActionType.values()[random.nextInt(ActionType.values().length)];
                    switch (actionType)
                    {
                        case SEARCH:
                            searchKeyWord = searchKeyWords[random.nextInt(4)];
                            break;
                        case CLICK:
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                            clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                            break;
                        case ORDER:
                            orderCategoryIds = getRandomStringArrs();
                            orderProductIds = getRandomStringArrs();
                            break;
                        case PAY:
                            payCategoryIds = getRandomStringArrs();
                            payProductIds = getRandomStringArrs();
                            break;
                        default:
                            break;
                    }
                    Row row = RowFactory.create(date, userId, sessionId,
                            pageid, actionTime, searchKeyWord,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds);
                    rows.add(row);
                }
            }
        }
        StructType type = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, false),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)));

        DataFrame df = sqlContext.createDataFrame(jsc.parallelize(rows), type);
        df.registerTempTable("user_visit_action");

        /**************测试用**************/
        List<Row> rows2 = df.toJavaRDD().take(1);
        for (Row row : rows2)
        {
            System.out.println(row);
        }
    }

    /**
     * 获取随机个数的id
     * 比如,获取 ids 个 click_category_id
     * @return 字符串ids
     */
    private static String getRandomStringArrs()
    {
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        int ids = random.nextInt(7);
        for (int i = 0; i < ids ; i++)
        {
            sb.append( String.valueOf(random.nextInt(100)));
            if(i<ids-1)
            {
                sb.append(",");
            }
        }
        return sb.toString();
    }
}