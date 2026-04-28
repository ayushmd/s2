import boto3
import pandas as pd
from io import BytesIO

BUCKET = "test"
PREFIX = ""
OUTPUT_FILE = "merged.csv"

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:8000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
    use_ssl=False
)

paginator = s3.get_paginator("list_objects_v2")

first = True

for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]

        if key.lower().endswith(".csv"):
            print(f"Processing {key}")

            response = s3.get_object(Bucket=BUCKET, Key=key)
            df = pd.read_csv(BytesIO(response["Body"].read()))

            df.to_csv(
                OUTPUT_FILE,
                mode="w" if first else "a",
                header=first,
                index=False
            )
            first = False

print(f"Saved merged file to {OUTPUT_FILE}")