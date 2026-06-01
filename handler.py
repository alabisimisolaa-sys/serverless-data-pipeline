import json
import os
import csv
import io
import uuid
import boto3

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
sns = boto3.client("sns")

TABLE = os.environ["TABLE_NAME"]
TOPIC = os.environ["TOPIC_ARN"]


def handler(event, context):
    """Triggered by SQS messages that carry S3 ObjectCreated events.

    For each new CSV under incoming/, parse the rows, write them to DynamoDB,
    copy the file to processed/, and publish an SNS notification. Raising on a
    bad file lets SQS retry, then route the message to the dead-letter queue.
    """
    table = ddb.Table(TABLE)
    total = 0

    for record in event["Records"]:
        body = json.loads(record["body"])
        for s3rec in body.get("Records", []):
            bucket = s3rec["s3"]["bucket"]["name"]
            key = s3rec["s3"]["object"]["key"]
            if not key.lower().endswith(".csv"):
                continue

            obj = s3.get_object(Bucket=bucket, Key=key)
            text = obj["Body"].read().decode("utf-8")
            rows = list(csv.DictReader(io.StringIO(text)))
            if not rows:
                raise ValueError("No rows parsed from " + key)

            with table.batch_writer() as batch:
                for row in rows:
                    item = {k: v for k, v in row.items() if v not in (None, "")}
                    item["recordId"] = str(uuid.uuid4())
                    item["sourceFile"] = key
                    batch.put_item(Item=item)

            total += len(rows)
            name = key.split("/")[-1]
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key="processed/" + name,
            )
            sns.publish(
                TopicArn=TOPIC,
                Subject="Pipeline: file processed",
                Message="Processed " + str(len(rows)) + " records from " + key,
            )

    print("Total records processed:", total)
    return {"processed": total}
