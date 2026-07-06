import json
import random
import uuid
from datetime import datetime, timezone

import boto3


S3_BUCKET = "de-saas-platform-san"

EVENT_TYPES = [
    "login",
    "logout",
    "create_page",
    "update_page",
    "delete_page",
    "upload_file",
    "download_file",
    "share_document",
    "invite_user"
]

DEVICES = [
    "Windows",
    "macOS",
    "Linux",
    "Android",
    "iOS",
    None
]

BROWSERS = [
    "Chrome",
    "Firefox",
    "Edge",
    "Safari",
    None
]

COUNTRIES = [
    "India",
    "United States",
    "United Kingdom",
    "Germany",
    "Canada",
    "Australia"
]

SUBSCRIPTION_PLANS = [
    "Free",
    "Basic",
    "Pro",
    "Enterprise"
]



s3_client = boto3.client("s3")


def generate_event():
    current_time = datetime.now(timezone.utc)

    return {
        "event_id": f"EVT{uuid.uuid4().hex[:8].upper()}",
        "user_id": random.randint(100, 500),
        "workspace_id": f"WS{random.randint(1, 50)}",
        "organization_id": f"ORG{random.randint(1, 20)}",
        "event_type": random.choice(EVENT_TYPES),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "country": random.choice(COUNTRIES),
        "subscription_plan": random.choice(SUBSCRIPTION_PLANS),
        "timestamp": current_time.isoformat().replace("+00:00", "Z")
    }

def lambda_handler(event, context):
    event_count = 100

    events = [generate_event() for _ in range(event_count)]

    payload = {
        "events": events
    }

    current_time = datetime.now(timezone.utc)

    year = current_time.strftime("%Y")
    month = current_time.strftime("%m")
    day = current_time.strftime("%d")

    file_name = f"events_{current_time.strftime('%Y%m%d_%H%M%S')}.json"

    s3_key = (
        f"raw/year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{file_name}"
    )

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Events generated successfully.",
                "s3_key": s3_key,
                "event_count": event_count
            }
        )
    }


