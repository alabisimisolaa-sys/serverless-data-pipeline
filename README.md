# Serverless Data Pipeline (Manual Console Build)

An asynchronous, event-driven serverless data pipeline built entirely by hand in the AWS Management Console to establish deep familiarity with core cloud services, resource-based permissions, and system observability.

## Architecture Overview

The pipeline implements a decoupled ingest-process-notify architecture:
1. **Ingestion:** A user uploads a `.csv` file into an **Amazon S3** bucket (`incoming/` prefix).
2. **Buffering & Decoupling:** S3 triggers an event notification that routes a message into an **Amazon SQS** main queue (`pipeline-queue`).
3. **Compute:** **AWS Lambda** polls the SQS queue, extracts the S3 metadata, retrieves the object, and processes the contents.
4. **Storage:** Cleanly parsed records are written in batches into an **Amazon DynamoDB** table (`pipeline-records`) using an on-demand capacity model.
5. **Notification & Observability:** * On successful execution, the Lambda publishes to an **Amazon SNS** topic (`pipeline-notify`) to send an email notification.
   * If an unhandled exception or corrupt schema is encountered, the message retries 3 times before being safely isolated into an **SQS Dead-Letter Queue (DLQ)**.
   * A **CloudWatch Alarm** watches the Lambda error metric and triggers a high-priority SNS alert.

##  Skills & Architectural Strategy ("Why I Built It This Way")

* **Decoupling with SQS:** Placing a message queue between S3 and Lambda buffers spiky workloads. If downstream processing slows down or experiences an outage, messages sit safely in flight rather than failing silently.
* **Cost-Optimization with DynamoDB On-Demand:** Configured database capacity to scale with requests rather than provisioning fixed throughput, ensuring zero baseline cost while idle.
* **Observability First:** Implemented explicit error definitions, custom visibility timeouts, a dedicated DLQ fallback pattern, and CloudWatch alarm triggers to ensure production issues are diagnosed automatically.

##  Production Troubleshooting & Optimizations

During verification, I systematically diagnosed and resolved two core infrastructure bottlenecks:
1. **Lambda Handler Resolution:** Fixed a `Runtime.HandlerNotFound` error by adjusting the runtime settings to map correctly to the explicit entry point (`lambda_function.handler`) instead of the default managed runtime wrapper.
2. **Timeout Extension:** Resolved function timeouts by increasing the execution ceiling from the 3-second default to `1 min 0 sec`, ensuring adequate headroom for batch operations and remote database writes.

## Verification Results

* **Happy Path:** Successfully processed multiple data batches (`test-orders.csv` and `sample-data.csv`), scaling dynamically to stream and parse tabular data perfectly into DynamoDB attribute arrays.
* **Resilience Testing:** Validated data parsing tolerance against unstructured file uploads, verifying system resilience before triggering error code handling.
