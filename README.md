
Hướng dẫn chạy ứng dụng

1. Khởi động Kafka

cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

Mở tab terminal mới:

cd ~/kafka
bin/kafka-server-start.sh config/server.properties

2. Khởi động HDFS và YARN (trên WSL)

Format lần đầu (nếu chưa format):

hdfs namenode -format

Bật Hadoop:

start-dfs.sh
start-yarn.sh

Kiểm tra:

jps

Truy cập HDFS UI: http://localhost:9870/

3. Chạy Producer Kafka

cd ~/project/kafka-code
python3 producer.py

Producer sẽ đọc file CSV và gửi từng dòng lên Kafka topic transactions, delay ngẫu nhiên 1 – 3 giây.

4. Chạy Spark Structured Streaming (Consumer)

Bật virtualenv:

cd ~/pyspark-env
source bin/activate

Chạy Spark consumer:

cd ~/project/kafka-code

spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /home/vivian/project/kafka-code/consumer.py

Spark sẽ đọc Kafka topic transactions, transform dữ liệu và ghi xuống HDFS tại /user/vivian/credit_card_all/.

5. Xóa batch CSV thủ công trên HDFS (nếu cần)

hadoop fs -rm /user/vivian/credit_card_all_merged.csv
hadoop fs -rm /user/vivian/credit_card_all/*


6. Chạy script push dữ liệu lên Power BI

Cài đặt thư viện cần thiết (nếu chưa cài):

pip install pyarrow fsspec hdfs requests

Chạy script push dữ liệu:

cd ~/project/kafka-code
python3 powerbi.py

Script sẽ đọc file CSV merged từ HDFS, phân tích và push lên Power BI Web Streaming Dataset.

8. Airflow Scheduler và Webserver

Khởi động Airflow:

airflow scheduler

Mở tab terminal mới:

airflow webserver

Truy cập Airflow Web UI: http://localhost:8080/

DAG chính:

merge_and_powerbi_dag: tự động xóa file merged cũ, merge CSV mới, và chạy powerbi.py mỗi ngày.

Tóm tắt pipeline:

Producer → Kafka → Spark Streaming → HDFS → Airflow → Power BI Web Dashboard

