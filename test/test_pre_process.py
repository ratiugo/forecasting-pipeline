"""
Pre process tests
"""
import os
from unittest import TestCase
import boto3
from moto import mock_s3
from forecast.pre_process import PreProcess

BUCKET = os.environ.get("FORECAST_S3_BUCKET", "fake-bucket")
client = boto3.client("s3")

@mock_s3
class TestPreProcess(TestCase):
    """
    Test cases for the pre process lambda function
    """

    @staticmethod
    def upload_input_data(bucket: str, input_dir: str) -> None:
        """
        Functionality to upload all files in `input_dir` to the provided `bucket`.
        """
        file_paths = []
        for path, _, files in os.walk(input_dir):
            for file_name in files:
                full_path = os.path.join(path, file_name)
                file_paths.append(full_path)

        for path in file_paths:
            key = os.path.relpath(path, input_dir)
            client.upload_file(Filename=path, Bucket=bucket, Key=key)

    def setUp(self) -> None:
        """
        Upload all files under test/mocked_s3_bucket to the mocked bucket
        """
        bucket_exists = False
        try:
            client.head_bucket(Bucket=BUCKET)
            bucket_exists = True
        except client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                bucket_exists = False
        if bucket_exists:
            client.delete_bucket(Bucket=BUCKET)
        client.create_bucket(Bucket=BUCKET)
        current_dir = os.path.dirname(__file__)
        mocked_s3_bucket_path = os.path.join(current_dir, "mocked_s3_bucket")

        self.upload_input_data(BUCKET, mocked_s3_bucket_path)

    def test_test(self) -> None:
        """
        Testing setup of test suite
        """
        test_name = "test"
        PreProcess(
            config_file=f"input/process_id_{test_name}/config.json"
        ).run()

    def tearDown(self) -> None:
        """
        Clean up the mocked S3 bucket
        """
        response = client.list_objects_v2(Bucket=BUCKET)
        if "Contents" in response:
            for obj in response["Contents"]:
                client.delete_object(Bucket=BUCKET, Key=obj["Key"])
        client.delete_bucket(Bucket=BUCKET)
