# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("sparksql example"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", "2"). \
        config("spark.sql.warehouse.dir", "hdfs://node1.itcast.cn:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node3.itcast.cn:9083"). \
        enableHiveSupport().getOrCreate()

    # 1 load data
    # drop data whose province value is null
    # drop data whose receivable > 10000
    # based on requirements, select only 5 columns
    df = spark.read.format("json").load("../../data/input/mini.json"). \
        dropna(thresh=1, subset=["storeProvince"]). \
        filter("storeProvince !='null'"). \
        filter("receivable < 10000"). \
        select("storeProvince", "storeID", "receivable", "dateTS", "payType")

    # TODO 1: sales of every province
    prov_sales_df = df.groupBy("storeProvince").sum("receivable"). \
        withColumnRenamed("sum(receivable)", "prov_sales"). \
        withColumn("prov_sales", F.round("prov_sales", 2)). \
        orderBy("prov_sales", ascending=False)

    prov_sales_df.show()

    # write in hive
    # prov_sales_df.write.mode("overwrite").saveAsTable("province_sale", "parquet")

    # TODO 2: find stores daily sale >1000 among top 3 province
    top3_province_df = prov_sales_df.limit(3).select('storeProvince'). \
        withColumnRenamed("storeProvince", "top3_storeProvince")

    top3_province_df_joined = df.join(top3_province_df,
                                      on=df["storeProvince"] == top3_province_df["top3_storeProvince"])

    top3_province_df_joined.persist(StorageLevel.MEMORY_AND_DISK)

    prov_hot_store_count = top3_province_df_joined.groupBy("storeProvince", "storeID",
                                                           F.from_unixtime(df["dateTS"].substr(0, 10),
                                                                           "yyyy-MM-dd").alias("day")). \
        sum("receivable").withColumnRenamed("sum(receivable)", "sale"). \
        filter("sale>1000"). \
        dropDuplicates(subset=["storeID"]). \
        groupBy("storeProvince").count()

    prov_hot_store_count.show()

    prov_hot_store_count.write.mode("overwrite").saveAsTable("prov_hot_store_count", "parquet")

    # TODO 3:avg order price of top 3 province
    top3_province_avg_order_df = top3_province_df_joined.groupBy("storeProvince").avg("receivable"). \
        withColumnRenamed("avg(receivable)", "avg_price"). \
        withColumn("avg_price", F.round("avg_price", 2)). \
        orderBy("avg_price", ascending=False)

    top3_province_avg_order_df.show()
    top3_province_avg_order_df.write.mode("overwrite").saveAsTable("top3_province_avg_order", "parquet")

    # TODO 4:proportion of different ways of payment in top 3 provinces
    top3_province_df_joined.createTempView("province_pay")

    # define a function to format the percent
    def format_number(percent):
        return str(round(percent*100,2))+"%"

    format_udf = F.udf(format_number,StringType())

    pay_type_df = spark.sql("""
    select storeProvince,payType,(count(payType)/total) as percent from
    (select storeProvince,payType,count(receivable) over(partition by storeProvince) as total from province_pay) as x
    group by 1,2,total
    """).withColumn("percent",format_udf("percent"))

    pay_type_df.show()
    pay_type_df.write.mode("overwrite").saveAsTable("pay_type_percent", "parquet")

    top3_province_df_joined.unpersist()
