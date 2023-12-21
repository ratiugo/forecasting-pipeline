"""
Module which takes a local CSV dataset and JSON configuration file, uploads them to S3, and 
runs the AWS forecasting pipeline with the provided files.
"""
import argparse
import json
from datetime import datetime
import uuid
from io import StringIO
import pandas as pd
import boto3

bucket = environ.get("FORECAST_S3_BUCKET")

class RunForecastStepFunction:
	"""
	Run the forecast step function after staging the data and configuration files to S3.
	"""
	def __init__(self, **kwargs):
		if not kwargs.get("config_file_name").endswith(".json"):
			raise ValueError("Invalid config file format. Must be a JSON file.")
		if not kwargs.get("data_file_name").endswith(".csv"):
			raise ValueError("Invalid data file format. Must be a CSV file.")

		self.config_file_name = kwargs.get("config_file_name")
		with open(self.config_file_name) as config_file:
			self.config = json.load(config_file)
		self.data_file_name = kwargs.get("data_file_name")
		self.data = pd.read_csv(self.data_file_name)
		self.process_id = None

	def run(self) -> RunForecastStepFunction:
		"""
		Run the pipeline
		"""
		(
			self.generate_process_id()
				.update_forecast_configuration_with_dataset_location()
				.stage_input_files()
				.run_step_function()
		)
		return self

	def generate_process_id(self) -> RunForecastStepFunction:
		"""
		Generate a unique ID to keep track of the process
		"""
		timestamp = datetime.utcnow().strftime("%Y%m%d")
		unique_id = uuid.uuid1()
		self.process_id = f"{timestamp}-{unique_id}"
		self.config["process_id"] = self.process_id

		return self

	def update_forecast_configuration_with_dataset_location(self) -> RunForecastStepFunction:
		"""
		Add the S3 file path of the dataset to the configuration file
		"""
		self.config["Dataset"]["file_name"] = f"{self.process_id}/input/{self.data_file_name}"

	@staticmethod
	def stage_data_to_s3(body: Any, key: str) -> None:
		"""
		Stage data to S3 bucket. Used for both the dataset CSV and configuration JSON files
		"""
		s3 = boto3.client("s3")
    	s3.put_object(Bucket=bucket, Key=key, Body=body)


	def stage_input_files(self) -> RunForecastStepFunction:
		"""
		Upload the CSV dataset and JSON configuration files to S3
		"""
		csv_buffer = StringIO()
		self.data.to_csv(csv_buffer, index=False)
		config = bytes(json.dumps(self.config, indent=4).encode("UTF-8"))

		self.stage_data_to_s3(body=csv_buffer.getvalue(),
							  key=f"{self.process_id}/input/{self.data_file_name}"
		)
		self.stage_data_to_s3(body=config,
							  key=f"{self.process_id}/input/{self.config_file_name}"
		)

		return self

	def run_step_function(self) -> RunForecastStepFunction:
		"""
		Run the forecasting step function, and return the status
		"""
		state_machine_arn = environ.get("STATE_MACHINE_ARN")
		session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "default"))
		client = session.client("stepfunctions")

		input_json = f"{{'config_file': '{f"{self.process_id}/input/{self.config_file_name}"}'}}"

		response = client.start_execution(
			stateMachineArn=state_machine_arn, input=input+json
		)
        execution_arn = response.get("executionArn")
        wait_time = 10
        status = "RUNNING"

        while status == "RUNNING":
        	print("Running...")
            time.sleep(wait_time)
            status = client.describe_execution(executionArn=execution_arn)["status"]

        print(f"Process {self.process_id} finished with status: {status}.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file-name", dest="config_file_name")
    parser.add_argument("--data-file-name", dest="data_file_name")

    RunForecastStepFunction(**vars(parser.parse_args())).run()