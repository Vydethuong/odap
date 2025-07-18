import subprocess
import pandas as pd
import io
import requests
import numpy as np
import os

# ========== CẤU HÌNH ==========
# File lưu số dòng đã push
ROW_COUNT_FILE = "/home/vivian/airflow/last_row_count.txt"

# Đường dẫn Power BI
PBI_PUSH_URL = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/45cae05b-c104-400e-ba25-51b4bb2aa2c4/rows?experience=power-bi&key=7YJDmnirAB%2BXqmTx0wpFxI1FhEIyKKntDEAcOkZ29LAIeK6gYvN%2FG3jQwTEKb8SyGDJjS6pI%2FxRiRMwBAJg5dQ%3D%3D"

# Đường dẫn file trên HDFS
hdfs_path = "/user/vivian/credit_card_all_merged.csv"

# ========== HÀM ĐỌC ROW COUNT ==========
def get_last_row_count():
    if os.path.exists(ROW_COUNT_FILE):
        with open(ROW_COUNT_FILE, "r") as f:
            count = f.read().strip()
            return int(count) if count.isdigit() else 0
    return 0

# ========== HÀM PUSH DATA ==========
def push_batches(data, label):
    batch_size = 50
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        res = requests.post(PBI_PUSH_URL, json=batch)
        print(f"{label} Batch {i // batch_size + 1}: {res.status_code}")
        if res.status_code != 200:
            print(res.text)


# ========== ĐỌC FILE HDFS ==========
output = subprocess.check_output(["hdfs", "dfs", "-cat", hdfs_path])
df = pd.read_csv(io.StringIO(output.decode("utf-8")))



# ========== XỬ LÝ DATA ==========
df.columns = [
    "user_id", "card_id", "datetime", "hour", "amount_usd",
    "merchant_name", "merchant_city", "merchant_state",
    "zip", "mcc", "year", "month", "day", "errors", "is_fraud"
]

df["is_fraud"] = df["is_fraud"].apply(lambda x: str(x).strip().lower() == "yes")
df["datetime"] = pd.to_datetime(df["datetime"], errors='coerce')
df = df.dropna(subset=["datetime"])
df.replace([np.inf, -np.inf], np.nan, inplace=True)
df.fillna(0, inplace=True)
df["merchant_name"] = df["merchant_name"].apply(lambda x: str(x)[:100])
df["datetime"] = df["datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

# ========== LỌC THEO SỐ DÒNG ==========
last_row_count = get_last_row_count()
print(f"Số dòng đã push trước đó: {last_row_count}")
print(f"Tổng số dòng hiện tại: {len(df)}")

df_new = df.iloc[last_row_count:]
print(f"Số dòng mới sẽ push: {len(df_new)}")

# ========== NẾU CÓ DATA MỚI THÌ PUSH ==========
if not df_new.empty:
    records = df_new[[
        "datetime", "hour", "user_id", "amount_usd",
        "merchant_name", "merchant_city", "is_fraud", "errors"
    ]].to_dict(orient="records")

    push_batches(records, "Transaction")

    # Cập nhật số dòng đã push
    with open(ROW_COUNT_FILE, "w") as f:
        f.write(str(len(df)))
else:
    print("Không có dữ liệu mới, không push lên Power BI.")


print("\n=================== MERCHANT_CITY & AMOUNT_USD ===================")
print(df[["user_id","merchant_name","merchant_city", "amount_usd","hour","is_fraud","errors"]])
print(f"Tổng số dòng: {len(df)}")
print("======================================================\n")




# ====================== FRAUD SUMMARY ======================

# Group theo merchant_city
df_fraud_city = df_new.groupby("merchant_city").agg(
    total_transactions=("user_id", "count"),
    total_fraud=("is_fraud", "sum")
).reset_index()

df_fraud_city["fraud_rate"] = round(df_fraud_city["total_fraud"] / df_fraud_city["total_transactions"] * 100, 2)
df_fraud_city["type"] = "city"
df_fraud_city["merchant_name"] = ""  # city không có merchant_name
df_fraud_city.rename(columns={"merchant_city": "merchant_city"}, inplace=True)

# Group theo merchant_name
df_fraud_merchant = df_new.groupby("merchant_name").agg(
    total_transactions=("user_id", "count"),
    total_fraud=("is_fraud", "sum")
).reset_index()

df_fraud_merchant["fraud_rate"] = round(df_fraud_merchant["total_fraud"] / df_fraud_merchant["total_transactions"] * 100, 2)
df_fraud_merchant["type"] = "merchant"
df_fraud_merchant["merchant_city"] = ""  # merchant không có city
df_fraud_merchant.rename(columns={"merchant_name": "merchant_name"}, inplace=True)

# Gộp lại
df_fraud_summary = pd.concat([df_fraud_city, df_fraud_merchant], ignore_index=True)

# Chỉ giữ các cột cần push
df_fraud_summary = df_fraud_summary[[
    "type", "merchant_name", "merchant_city", "total_transactions", "total_fraud", "fraud_rate"
]]

# In kiểm tra
print("\n========== FRAUD SUMMARY ==========")
print(df_fraud_summary)

# Chuẩn bị dữ liệu push
fraud_summary_records = df_fraud_summary.to_dict(orient="records")

# ====================== PUSH LÊN POWER BI ======================


PBI_PUSH_URL_FRAUD = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/d88f12fa-ef2c-4e5a-ac87-c27c380c053f/rows?experience=power-bi&key=18boQxLtTGGAA1CudrXHON9fqP%2F6k3NDZe4SO4nYNmNYpQs30UDos%2Bq2Plm78v58fghcWCfdh%2Fuax2%2FZUDtACQ%3D%3D"

# Hàm push batches cho Fraud Summary
def push_batches_fraud(data, label):
    batch_size = 50
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        res = requests.post(PBI_PUSH_URL_FRAUD, json=batch)
        print(f"{label} Batch {i // batch_size + 1}: {res.status_code}")
        if res.status_code != 200:
            print(res.text)

# Push Fraud Summary
push_batches_fraud(fraud_summary_records, "Fraud Summary")



# ====================== LIÊN TIẾP TRONG THỜI GIAN NGẮN ======================

# Chuyển datetime về lại datetime object để xử lý
df_datetime = df_new.copy()
df_datetime["datetime"] = pd.to_datetime(df_datetime["datetime"], errors='coerce')

# Sort theo user_id và datetime
df_datetime = df_datetime.sort_values(by=["user_id", "datetime"])

# Tính khoảng cách giữa các giao dịch của cùng 1 user
df_datetime["time_diff_minutes"] = df_datetime.groupby("user_id")["datetime"].diff().dt.total_seconds() / 60

# Lọc những dòng có khoảng cách < 5 phút
df_short_gap = df_datetime[df_datetime["time_diff_minutes"] <= 5]

# Đếm số lần xảy ra với mỗi user
df_user_short_gap = df_short_gap.groupby("user_id").agg(
    short_gap_count=("time_diff_minutes", "count")
).reset_index()

# Chỉ giữ những user có ít nhất 2 lần (tức là ≥ 3 giao dịch liên tiếp)
df_user_short_gap = df_user_short_gap[df_user_short_gap["short_gap_count"] >= 2]

# Nếu không có user nào thì tạo dataframe trống
if df_user_short_gap.empty:
    df_user_short_gap = pd.DataFrame(columns=["user_id", "short_gap_count"])

# In kiểm tra
print("\n========== SHORT TIME GAPS ==========")
print(df_user_short_gap)

# Chuẩn bị push
short_gap_records = df_user_short_gap.to_dict(orient="records")

# ====================== PUSH LÊN DATASET KHÁC ======================

PBI_PUSH_URL_SHORT_GAP = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/8f6ec20a-9acb-4ed6-b4ba-6e76b5025d9e/rows?experience=power-bi&key=qqzUIfTB08XivD8o0%2Bdq3yDVZ05gq4IrQ%2FclCgsH%2BLkIECJmDJ%2BWWysvlrrUn9o8xY9VlEwv52KYEQNgFOu7uA%3D%3D"

def push_batches_short_gap(data, label):
    batch_size = 50
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        res = requests.post(PBI_PUSH_URL_SHORT_GAP, json=batch)
        print(f"{label} Batch {i // batch_size + 1}: {res.status_code}")
        if res.status_code != 200:
            print(res.text)

# Push Short Time Gaps
push_batches_short_gap(short_gap_records, "Short Gaps")




# ====================== WEEKDAY VS WEEKEND ======================

# Chuyển datetime về lại datetime object để xử lý
df_week = df_new.copy()
df_week["datetime"] = pd.to_datetime(df_week["datetime"], errors='coerce')

# Thêm cột is_weekend: True nếu là thứ 7 (5) hoặc CN (6)
df_week["is_weekend"] = df_week["datetime"].dt.weekday >= 5

# Group theo is_weekend và hour
df_week_summary = df_week.groupby(["is_weekend", "hour"]).agg(
    total_transactions=("user_id", "count"),
    total_amount=("amount_usd", "sum")
).reset_index()

# Đổi kiểu bool thành chuỗi cho dễ đọc trong Power BI
df_week_summary["is_weekend"] = df_week_summary["is_weekend"].apply(lambda x: "Weekend" if x else "Weekday")

# In kiểm tra
print("\n========== WEEKDAY VS WEEKEND ==========")
print(df_week_summary)

# Chuẩn bị push
week_summary_records = df_week_summary.to_dict(orient="records")

# ====================== PUSH LÊN DATASET KHÁC ======================

PBI_PUSH_URL_WEEK = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/0c8568aa-3896-4e07-b736-899ed4fcc96a/rows?experience=power-bi&key=9Eh8M3uuGF7iCfb6GErN3%2B5ZUHlSYpLvvekjo%2FS%2F%2FKNsbTypqjnyzGEjXPRcra6Jb5w2smEJ4YSP61w3xYUrvw%3D%3D"

def push_batches_week(data, label):
    batch_size = 50
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        res = requests.post(PBI_PUSH_URL_WEEK, json=batch)
        print(f"{label} Batch {i // batch_size + 1}: {res.status_code}")
        if res.status_code != 200:
            print(res.text)

# Push Weekday vs Weekend Summary
push_batches_week(week_summary_records, "Weekday vs Weekend")



# ====================== ERROR & FRAUD SUMMARY ======================

# Chuẩn hóa dữ liệu
# ====================== ERROR & FRAUD SUMMARY ======================
df_ab = df_new.copy()
df_ab["is_fraud"] = df_ab["is_fraud"].apply(lambda x: True if str(x).strip().lower() == "true" else False)
df_ab["errors"] = df_ab["errors"].astype(str)

df_user = df_ab.groupby("user_id").agg(
    total_errors=("errors", lambda x: sum(e != "0" for e in x)),
    total_fraud=("is_fraud", lambda x: sum(x))
).reset_index()

avg_errors = df_user["total_errors"].mean()
avg_fraud = df_user["total_fraud"].mean()

abnormal_users = df_user[
    (df_user["total_errors"] > avg_errors) |
    (df_user["total_fraud"] > avg_fraud)
].reset_index(drop=True)


print("\n========== ABNORMAL USERS ==========")
print(abnormal_users)

# Chuẩn bị dữ liệu push
records = abnormal_users.to_dict(orient="records")

# ====================== PUSH LÊN DATASET KHÁC ======================

PBI_PUSH_URL_ABNORMAL = "https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/6077a648-0a7e-4a6a-b33e-7abf699e0e14/rows?experience=power-bi&key=aTkPTYKS%2BDKl94vtUd4%2FekujFnJirxEoFsxMKgxIfEZ%2FUHiX1hdKPxdhQv12cnP63406PeTF5qsZ9jMliNxj3Q%3D%3D"

# Push theo batch
def push_batches_abnormal(data, label):
    batch_size = 50
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        res = requests.post(PBI_PUSH_URL_ABNORMAL, json=batch)
        print(f"{label} Batch {i // batch_size + 1}: {res.status_code}")
        if res.status_code != 200:
            print(res.text)

# Gọi push
push_batches_abnormal(records, "Abnormal Users")





