import requests

event = {
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49641808440514706213932692080310623236477433591655038978",
                "data": "ewogICAgICAgICJyaWRlIjogewogICAgICAgICAgICAiUFVMb2NhdGlvbklEIjogMTMwLAogICAgICAgICAgICAiRE9Mb2NhdGlvbklEIjogMjA1LAogICAgICAgICAgICAidHJpcF9kaXN0YW5jZSI6IDMuNjYKICAgICAgICB9LCAKICAgICAgICAicmlkZV9pZCI6IDE1NgogICAgfQ==",
                "approximateArrivalTimestamp": 1687027572.021
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49641808440514706213932692080310623236477433591655038978",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::340951083884:role/lambda-kinesis-role",
            "awsRegion": "us-east-1",
            "eventSourceARN": "arn:aws:kinesis:us-east-1:340951083884:stream/ride_events"
        }
    ]
}

url = "http://localhost:8080/2015-03-31/functions/function/invocations"
response = requests.post(url, json=event)
print(response.json())
