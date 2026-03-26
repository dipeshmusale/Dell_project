import boto3
import extract_msg
import json
import email

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        download_path = '/tmp/email_file'
        s3.download_file(bucket, key, download_path)

        data = {}

        # =========================
        # TRY MSG FORMAT
        # =========================
        try:
            msg = extract_msg.Message(download_path)
            data = {
                "subject": msg.subject,
                "sender": msg.sender,
                "date": str(msg.date),
                "body": msg.body,
                "file_type": "msg"
            }

        # =========================
        # FALLBACK TO EML
        # =========================
        except Exception:
            with open(download_path, "r", encoding="utf-8", errors="ignore") as f:
                eml = email.message_from_file(f)

            data = {
                "subject": eml.get("subject"),
                "sender": eml.get("from"),
                "date": eml.get("date"),
                "body": str(eml.get_payload()),
                "file_type": "eml"
            }

        # Output path
        output_key = key.replace("raw/msg/", "raw/converted/") + ".json"

        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=json.dumps(data)
        )

        return "Success"

    except Exception as e:
        print("Error:", str(e))
        raise e