import os
import io
import boto3
import pandas as pd

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

def main():
    s3 = get_s3_client()
    bucket = "final-project"
    dates = set()
    
    print("Сканирование батчей для сбора уникальных дат...")
    for i in range(1, 11):
        key = f"transactions_batch_{i}.csv"
        print(f"Чтение {key}...")
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()), usecols=["transaction_dt"])
        
        # Страховка от пустых строк
        df = df.dropna(subset=["transaction_dt"])
        unique_dates = pd.to_datetime(df["transaction_dt"]).dt.strftime("%Y-%m-%d").unique()
        dates.update(unique_dates)
        
    print("\nУникальные даты в источнике:")
    print(sorted(dates))

if __name__ == "__main__":
    main()
