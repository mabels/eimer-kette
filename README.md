# s3-streaming-lister
A high performance s3 listobjectv2 implementation

Docker Container found here:

https://gallery.ecr.aws/mabels/s3-streaming-lister

Example usages:

Export all s3 file names in a SQLite file (file.sql):
```
./s3-streaming-lister \
    --bucket YOUR_BUCKET_NAME \
    --format sqlite \
    --sqliteCleanDb \
    --strategy letter \
    --delimiter ""
```
Note: The strategy letter should be used if the bucket has no subdirectories. The default strategy is delimiter with "/" as default.

Use the generated file.sql and simulate S3 Bucket "ObjectCreated:Put" events by writing messages to SQS:
```
./s3-streaming-lister \
    --bucket YOUR_BUCKET_NAME  \ 
    --frontend sqlite  \
    --outputSqsUrl https://YOUR_QUEUE_URL  \
    --outputSqsMaxMessageSize 20000 \
    --format sqs
```

Alternatively, you can export all file names directly to your SQS without the need for an intermediate persistence step:
```
./s3-streaming-lister \
    --bucket YOUR_BUCKET_NAME  \ 
    --outputSqsUrl https://YOUR_QUEUE_URL  \
    --outputSqsMaxMessageSize 20000 \
    --format sqs
```

List all possible parameters:
```
./s3-streaming-lister --help
```
