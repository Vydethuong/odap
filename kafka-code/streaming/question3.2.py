from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, concat_ws, to_timestamp, hour, count
from urllib.parse import urlparse
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import requests

def delete_old_batches(spark, base_path):
    """
    Xóa hết thư mục base_path trên HDFS (nếu có) và tạo lại rỗng.
    Ví dụ base_path = "hdfs://localhost:9000/home/vivian/creditcard_analytics/transactions_by_hour"
    """
    # Lấy Hadoop Configuration từ SparkContext
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
    # Phân tích URI để lấy host:port
    parsed = urlparse(base_path)
    uri = spark._jvm.java.net.URI.create(f"{parsed.scheme}://{parsed.netloc}")
    
    # Lấy FileSystem đúng của HDFS
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    
    # Tạo Path object cho thư mục gốc
    Path = spark._jvm.org.apache.hadoop.fs.Path
    directory = Path(base_path)

    # Nếu tồn tại, xóa đệ quy
    if fs.exists(directory):
        fs.delete(directory, True)

    # Tạo lại thư mục gốc trống
    fs.mkdirs(directory)


def main():
    # 1. Tạo SparkSession
    spark = SparkSession.builder \
        .appName("Question1_HourlyTransactionAnalysis") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 2. Xóa batch cũ ngay khi Spark bắt đầu
    base_hdfs = "hdfs://localhost:9000/home/vivian/creditcard_analytics/transactions_by_hour"
    delete_old_batches(spark, base_hdfs)

    # 3. Đọc dữ liệu từ Kafka (chỉ từ các message mới)
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "csv_topic") \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Chuyển "value" sang STRING và split theo dấu phẩy
    df_value = df_raw.selectExpr("CAST(value AS STRING) AS csv_line")
    df_split = df_value.select(split(col("csv_line"), ",").alias("fields"))

    # 5. Đặt tên cột (bỏ '\ufeff' nếu header xuất hiện)
    columns = [
        "User", "Card", "Year", "Month", "Day", "Time", "Amount",
        "Use_Chip", "Merchant_Name", "Merchant_City", "Merchant_State",
        "Zip", "MCC", "Errors", "Is_Fraud"
    ]
    credit_schema = StructType([
        StructField("User", StringType(), True),
        StructField("Card", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Day", IntegerType(), True),
        StructField("Time", StringType(), True),  # Hoặc TimestampType nếu muốn
        StructField("Amount", StringType(), True),  # Hoặc FloatType nếu muốn ép kiểu
        StructField("Use Chip", StringType(), True),
        StructField("Merchant Name", StringType(), False),
        StructField("Merchant City", StringType(), True),
        StructField("Merchant State", StringType(), True),
        StructField("Zip", StringType(), True),
        StructField("MCC", IntegerType(), True),
        StructField("Errors?", StringType(), True),
        StructField("Is Fraud?", StringType(), True)
    ])
    for i, name in enumerate(columns):
        df_split = df_split.withColumn(name, col("fields")[i])

    # 6. Lọc bỏ header (nếu có) và Ép kiểu chính xác theo schema
    df_no_header = df_split.filter(col("User") != '\ufeffUser')
    StreamingDF = df_no_header.select([
        col(name).cast(credit_schema[i].dataType).alias(name)
        for i, name in enumerate(columns)
    ])

    # 7. Lấy tỷ giá USD-VND ngày hôm nay từ api
    response = requests.get("https://open.er-api.com/v6/latest/USD")
    if response.status_code == 200:
        data = response.json()
        vnd_rate = data["rates"]["VND"]

    # 8. Cột Amount đang ở định dạng "$x.y", do vậy phải loại bỏ ký tự "$" và chuyển về kiểu số double
    StreamingDF = StreamingDF.withColumn(
        "Amount_clean", regexp_replace("Amount", "\\$", "").cast("double")
    )

    # 9. Tính tổng số tiền giao dịch groupBy theo "Merchant Name", "Merchant City", 2 tỷ giá USD, VND
    MerchantAmount = (
        StreamingDF
        .groupBy("Merchant Name", "Merchant City")
        .agg(sum("Amount_clean").alias("Total_USD"))
        .withColumn("Total_VND", round(col("Total_USD") * vnd_rate, 0).cast("long"))
        .orderBy(col("Total_USD").desc())
        .select(
            col("Merchant Name"),
            col("Merchant City"),
            round(col("Total_USD"), 2).alias("Total_Transactions_USD"),
            col("Total_VND").alias("Total_Transactions_VND")
        )
    )

    # 10. Hàm ghi mỗi batch ra Parquet trên HDFS
    def write_to_parquet(batch_df, batch_id):
        hdfs_path = (
            "hdfs://localhost:9000/"
            "home/vivian/creditcard_analytics/transactions_by_hour/"
            f"batch_{batch_id}"
        )
        batch_df.write.mode("overwrite").parquet(hdfs_path)

    # 11. Khởi streaming query
    query = MerchantAmount.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_to_parquet) \
        .option(
            "checkpointLocation",
            "/home/vivian/project/kafka-code/output/checkpoints/transactions_by_hour/"
        ) \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
