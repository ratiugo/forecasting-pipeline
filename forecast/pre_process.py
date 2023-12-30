"""
Pipeline to process the input dataset and configuration files, preparing for the actual forecasting 
pipeline.
"""
from __future__ import annotations
import io
from os import environ
from abc import ABC
import json
from typing import Union, Any
import pandas as pd
import boto3

BUCKET = environ.get("FORECAST_S3_BUCKET", "fake-bucket")
s3 = boto3.client("s3")


class PreProcess(ABC):
    """
    Process the CSV dataset into the shape required by the forecasting lambda
    """

    def __init__(self, **kwargs):
        print(kwargs)
        self.config = self.get_s3_object(bucket=BUCKET, key=kwargs.get("config_file"), _type = "json")
        self.dataset = None

    @staticmethod
    def put_s3_object(bucket: str, key: str, _object: Any) -> None:
        """
        Put an object in the specified S3 bucket
        """
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(_object))

    @staticmethod
    def get_s3_object(bucket: str, key: str, _type: str) -> Any:
        """
        Get an object from the specified S3 bucket
        """
        data = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        if _type == "csv":
            output = pd.read_csv(io.StringIO(data))
        if _type == "json":
            output = json.loads(data)

        return output

    def run(self) -> PreProcess:  # pylint: disable=undefined-variable
        """
        Run the pipeline
        """
        (self.load_dataset().create_model_dataset())

        return self

    def load_dataset(self) -> PreProcess:  # pylint: disable=undefined-variable
        """
        Read the S3 location of the CSV dataset and load it as a pandas dataframe
        """
        self.dataset = {
            "meta_data": self.config["dataset"]["meta_data"],
            "data": self.get_s3_object(bucket=BUCKET, key=self.config["dataset"]["file_name"], _type="csv")
        }
        print(self.dataset)
        return self

    def create_model_dataset(self) -> PreProcess:  # pylint: disable=undefined-variable
        """
        Create individual model datasets
        """
        model_dataset = {"config": self.config["config"]}
        model_dataset.update(self.dataset)

        if "group_models_by" in model_dataset["meta_data"].keys():
            model_dataset["model_group"] = {}
            model_dataset = [self.split_model_dataset(model_dataset)]

    def split_model_dataset(self, model: dict) -> Union[list, dict]:
        """
        Split data into grouped models
        """
        columns = model["meta_data"].get("group_models_by", [])
        grouped_data = model["data"].groupby(columns)

        models = []
        for group, data in grouped_data:
            model_tmp = {
                "config": model["config"],
                "model_group": dict(zip(columns, group)),
                "meta_data": model["meta_data"],
                "data": data,
            }
            models.append(model_tmp)

        return models


# pylint: disable=unused-argument
def pre_process_handler(event: Any, context: Any) -> str:
    """
    Lambda handler to run the preprocess pipeline
    """
    return PreProcess(config_file=event.get("config_file")).run()
