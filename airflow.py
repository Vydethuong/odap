from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import subprocess

# ================== CẤU HÌNH ==================
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
}

# ================== HÀM XÓA FILE MERGED CSV ==================
def delete_merged_csv():
    cmd = 'hadoop fs -rm -f /user/vivian/credit_card_all_merged.csv'
    result = subprocess.run(['bash', '-c', cmd], capture_output=True, text=True)
    if result.returncode == 0:
        print("Xóa file merged cũ thành công (nếu có).")
    else:
        print(f"Lỗi xóa file hoặc file không tồn tại: {result.stderr}")

# ================== HÀM MERGE CSV ==================
def merge_csv_on_hadoop():
    cmd = 'hadoop fs -cat /user/vivian/credit_card_all/* | hadoop fs -put -f - /user/vivian/credit_card_all_merged.csv'
    result = subprocess.run(['bash', '-c', cmd], capture_output=True, text=True)
    if result.returncode == 0:
        print("Merge CSV thành công")
    else:
        print(f"Lỗi merge CSV: {result.stderr}")

# ================== ĐỊNH NGHĨA DAG ==================
with DAG(
    dag_id='merge_and_powerbi_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 0: Xóa file merged cũ (nếu có)
    delete_task = PythonOperator(
        task_id='delete_merged_csv_task',
        python_callable=delete_merged_csv,
    )
    # Task 1: Merge CSV trên Hadoop
    merge_task = PythonOperator(
        task_id='merge_csv_task',
        python_callable=merge_csv_on_hadoop,
    )

    # Task 2: Chạy powerbi.py
    powerbi_task = BashOperator(
        task_id='run_powerbi_task',
        bash_command='python3 /home/vivian/project/kafka-code/powerbi.py',
    )

    # Chạy theo thứ tự
    delete_task >> merge_task >> powerbi_task
