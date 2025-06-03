from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, concat_ws, to_timestamp, hour, count
from urllib.parse import urlparse

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
    for i, name in enumerate(columns):
        df_split = df_split.withColumn(name, col("fields")[i])

    # 6. Lọc bỏ header (nếu có)
    df_no_header = df_split.filter(col("User") != '\ufeffUser')

    # 7. Tạo cột "timestamp" với pattern "d-M-yyyy H:mm"
    df_parsed = df_no_header.withColumn(
        "timestamp",
        to_timestamp(
            concat_ws(" ",
                      concat_ws("-", col("Day"), col("Month"), col("Year")),
                      col("Time")
            ),
            "d-M-yyyy H:mm"
        )
    )

    # 8. Lọc bỏ những dòng không parse được timestamp
    df_valid = df_parsed.filter(col("timestamp").isNotNull())

    # 9. Tạo cột "hour" và đếm số giao dịch theo giờ
    df_hourly = df_valid.withColumn("hour", hour(col("timestamp"))) \
        .groupBy("hour") \
        .agg(count("*").alias("transaction_count"))

    # 10. Hàm ghi mỗi batch ra Parquet trên HDFS
    def write_to_parquet(batch_df, batch_id):
        hdfs_path = (
            "hdfs://localhost:9000/"
            "home/vivian/creditcard_analytics/transactions_by_hour/"
            f"batch_{batch_id}"
        )
        batch_df.write.mode("overwrite").parquet(hdfs_path)

    # 11. Khởi streaming query
    query = df_hourly.writeStream \
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
