from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf, to_timestamp, hour
from pyspark.sql.types import IntegerType, DoubleType, StringType

# -------------------------------------------------
# 1. Hàm parse Amount (chuỗi như "$123.45") → float USD
# -------------------------------------------------
def parse_amount_usd(amount_str):
    """
    Nhận amount_str: ví dụ "$134.09" hoặc "$1,234.56"
    Trả về float (vd: 134.09, 1234.56), hoặc None nếu parse lỗi.
    """
    try:
        cleaned = amount_str.replace("$", "").replace(",", "")
        return float(cleaned)
    except:
        return None

parse_amount_udf = udf(parse_amount_usd, DoubleType())

# -------------------------------------------------
# 2. Hàm kết hợp ngày giờ (Day, Month, Year, Time) → chuỗi "dd/MM/yyyy HH:mm:ss"
# -------------------------------------------------
def combine_to_datetime_str(day, month, year, time_str):
    """
    Nhận day:int, month:int, year:int, time_str:str (vd: "6:21" hoặc "17:45" hoặc "9:35:00"),
    Trả về chuỗi "dd/MM/yyyy HH:mm:ss" với giờ/phút/giây đủ 2 chữ số.
    """
    d = f"{int(day):02d}"
    m = f"{int(month):02d}"
    y = f"{int(year):04d}"
    parts = time_str.split(":")

    # Nếu chỉ có "H:MM" hoặc "HH:MM", thêm ":00" cho giây
    if len(parts) == 2:
        parts.append("00")

    h = f"{int(parts[0]):02d}"
    mi = f"{int(parts[1]):02d}"
    s = f"{int(parts[2]):02d}"

    return f"{d}/{m}/{y} {h}:{mi}:{s}"

combine_dt_udf = udf(combine_to_datetime_str, StringType())

# -------------------------------------------------
# 3. Khởi tạo SparkSession
# -------------------------------------------------
spark = SparkSession.builder \
    .appName("KafkaToHDFS_Consumer_WithHeader") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# 4. Đọc stream từ Kafka
# -------------------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Cast giá trị 'value' từ binary → STRING (mỗi dòng CSV)
raw_df = kafka_df.selectExpr("CAST(value AS STRING) as csv_line")

# -------------------------------------------------
# 5. Tách các trường từ chuỗi CSV (KHÔNG lọc fraud/lỗi)
#    Giả sử header gốc: 
#    User,Card,Year,Month,Day,Time,Amount,Use Chip,Merchant Name,Merchant City,Merchant State,Zip,MCC,Errors?,Is Fraud?
# -------------------------------------------------
split_cols = split(col("csv_line"), ",")

parsed_df = raw_df.select(
    split_cols.getItem(0).cast(IntegerType()).alias("user_id"),
    split_cols.getItem(1).cast(IntegerType()).alias("card_id"),
    split_cols.getItem(2).cast(IntegerType()).alias("year"),
    split_cols.getItem(3).cast(IntegerType()).alias("month"),
    split_cols.getItem(4).cast(IntegerType()).alias("day"),
    split_cols.getItem(5).alias("time_str"),        # ví dụ "6:21" hoặc "9:35:00"
    split_cols.getItem(6).alias("amount_str"),      # ví dụ "$134.09"
    split_cols.getItem(7).alias("use_chip"),
    split_cols.getItem(8).alias("merchant_name"),
    split_cols.getItem(9).alias("merchant_city"),
    split_cols.getItem(10).alias("merchant_state"),
    split_cols.getItem(11).alias("zip"),
    split_cols.getItem(12).alias("mcc"),
    split_cols.getItem(13).alias("errors"),
    split_cols.getItem(14).alias("is_fraud")
)

# -------------------------------------------------
# 6. Parse datetime và parse Amount → USD, thêm cột Hour
# -------------------------------------------------
with_datetime_df = parsed_df.withColumn(
    "datetime_str",
    combine_dt_udf(
        col("day"), col("month"), col("year"), col("time_str")
    )
).withColumn(
    "datetime",
    to_timestamp(col("datetime_str"), "dd/MM/yyyy HH:mm:ss")
)

with_amount_df = with_datetime_df.withColumn(
    "amount_usd",
    parse_amount_udf(col("amount_str"))
)

processed_df = with_amount_df.withColumn(
    "hour", hour(col("datetime"))
)

# -------------------------------------------------
# 7. Chọn cột đầu ra (bỏ các cột tạm)
# -------------------------------------------------
output_df = processed_df.select(
    col("user_id"),
    col("card_id"),
    col("datetime"),
    col("hour"),
    col("amount_usd"),
    col("merchant_name"),
    col("merchant_city"),
    col("merchant_state"),
    col("zip"),
    col("mcc"),
    col("year"),
    col("month"),
    col("day"),
    col("errors"),
    col("is_fraud")
)

# -------------------------------------------------
# 8. Hàm ghi mỗi micro-batch với header vào HDFS
# -------------------------------------------------
def write_batch_no_header(df_batch, epoch_id):
    (df_batch
       .write
       .mode("append")
       .option("header", "false")  # Không ghi header mỗi file
       .csv("hdfs://localhost:9000/user/vivian/credit_card_all")
    )


# -------------------------------------------------
# 9. Ghi stream dùng foreachBatch
# -------------------------------------------------
query = output_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch_no_header) \
    .option("checkpointLocation", "/home/vivian/checkpoint/consumer_no_header") \
    .start()

query.awaitTermination()
