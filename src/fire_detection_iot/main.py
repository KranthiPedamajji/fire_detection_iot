from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import JSONResponse
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO
import traceback
from pathlib import Path
from os import getenv
from dotenv import load_dotenv

app = FastAPI()

# --- AWS S3 Configuration ---
# AWS_ACCESS_KEY = "AKIA3JQKMP2EUOG7SZYO"  # Replace with your access key
# AWS_SECRET_KEY = "CvTgo6gad9yazD/8KVQ4ClemNw4q6bB7WWa/NanS"  # Replace with your secret key
# BUCKET_NAME = "wildfire-project-data"
# ENDPOINT_ARN = (
#     "arn:aws:rekognition:us-east-2:776337129097:"
#     "project/WildfireDet/version/WildfireDet.2025-05-07T20.23.41/1746663820069"
# )
# Point to the .env in the project root (works even when app runs elsewhere)
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

AWS_ACCESS_KEY = getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = getenv("AWS_SECRET_KEY")
BUCKET_NAME    = getenv("BUCKET_NAME")
ENDPOINT_ARN   = getenv("ENDPOINT_ARN")


# --- boto3 client ---
s3 = boto3.client("s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

rek = boto3.client(
    "rekognition",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="us-east-2"
)

@app.post("/upload")
async def upload_data(
    image: UploadFile = File(...),
    temperature: float = Form(...),
    humidity: float = Form(...),
    alert: bool = Form(...)
):
    try:
        # Timestamp
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

        # Upload image to S3
        image_filename = f"images/{timestamp}.jpg"
        image_content = await image.read()
        s3.put_object(Bucket=BUCKET_NAME, Key=image_filename, Body=image_content, ContentType="image/jpeg")
        image_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{image_filename}"

        # 2. Run Rekognition inference
        resp = rek.detect_custom_labels(
                    ProjectVersionArn=ENDPOINT_ARN,
                    Image={
                        "S3Object": {
                        "Bucket": BUCKET_NAME,
                        "Name":  image_filename,
                        }
                    },
                MinConfidence=0
            )
        
        # Decide yourself whether it's a fire
        THRESHOLD_FIRE = 40.0
        fire_detected = any(
            lbl["Name"].lower() == "fire" and lbl["Confidence"] >= THRESHOLD_FIRE
            for lbl in resp["CustomLabels"]
        )


       # 3) Build a single‐row DataFrame for metadata
        row_df = pd.DataFrame(
            [[timestamp, temperature, humidity, alert, fire_detected, image_url]],
            columns=["timestamp", "temperature", "humidity", "alert", "fire_detected", "image_s3_url"]
        )

        # 4) Write that row to an in‐memory CSV
        buffer = StringIO()
        row_df.to_csv(buffer, index=False)
        buffer.seek(0)

        # 5) Upload a unique CSV file per request
        csv_key = f"metadata/{timestamp}.csv"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=csv_key,
            Body=buffer.getvalue(),
            ContentType="text/csv"
        )
        print("Uploaded metadata file", csv_key)

        return JSONResponse(
            status_code=200,
            content={"message": "Upload successful", "image_url": image_url, "fire_detected": fire_detected}
        )

    except Exception as e:
        # Print full traceback to console
        traceback.print_exc()
        return JSONResponse(status_code=500, content={
            "error": str(e)})
