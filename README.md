# s3-streaming-lister
A high performance s3 listobjectv2 implementation

Docker Container found here:

https://gallery.ecr.aws/mabels/s3-streaming-lister

Example usages:

Export all s3 object names to an SQLite file (file.sql):
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
    --bucket YOUR_BUCKET_NAME \ 
    --frontend sqlite  \
    --outputSqsUrl https://YOUR_QUEUE_URL \
    --outputSqsMaxMessageSize 20000 \
    --format sqs
```

Alternatively, you can export all object names directly to your SQS without the need for an intermediate persistence step:
```
./s3-streaming-lister \
    --bucket YOUR_BUCKET_NAME  \ 
    --outputSqsUrl https://YOUR_QUEUE_URL  \
    --outputSqsMaxMessageSize 20000 \
    --format sqs
```

If you only want to count all objects in an S3 bucket:

```
./s3-streaming-lister --bucket YOUR_BUCKET_NAME > /dev/null
```

The output is something like:
```
Now=2021-12-12T18:01:21Z Total=21176538/0  ListObjectsV2=942624/0/0/0.026   ListObjectsV2Input=942624/0/0   NewFromConfig=16/0/0/0.000
```

Delete objects from an S3 Bucket:
```
./s3-streaming-lister 
    --bucket YOUR_BUCKET_NAME  \
    --format s3delete \
    --outputS3DeleteWorkers 3
```

List all possible parameters:
```
./s3-streaming-lister --help
```

