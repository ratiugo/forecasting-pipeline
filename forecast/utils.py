"""
Utilities module to be shared by any forecasting step.
"""
import boto3 

def put_s3_object(bucket: str, key: str, _object: Any) -> None:
    """
    Put an object in the specified S3 bucket
    """
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(_object))

def get_s3_object(bucket: str, key: str) -> Any:
    """
    Get an object from the specified S3 bucket
    """
    s3 = boto3.client("s3")  
    return json.loads(
        s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    )
