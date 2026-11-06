import os
import boto3

def get_s3_client():
    """Возвращает клиент S3, используя переменные окружения."""
    return boto3.client(
        "s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

def main():
    s3 = get_s3_client()
    bucket = "final-project"
    
    print(f"--- Объекты в бакете {bucket} ---")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for o in page.get("Contents", []):
            print(f"{o['Key']:<40} {o['Size']} bytes")

    print("\n--- Первые 400 байт currencies_history.csv ---")
    obj = s3.get_object(Bucket=bucket, Key="currencies_history.csv")
    print(obj["Body"].read(400).decode("utf-8"))

    print("\n--- Поиск первого батча транзакций ---")
    response = s3.list_objects_v2(Bucket=bucket, Prefix="transactions_batch_")
    if "Contents" in response:
        first_key = response["Contents"][0]["Key"]
        print(f"--- Первые 400 байт {first_key} ---")
        obj = s3.get_object(Bucket=bucket, Key=first_key)
        print(obj["Body"].read(400).decode("utf-8"))
    else:
        print("Батчи транзакций не найдены.")

if __name__ == "__main__":
    main()
