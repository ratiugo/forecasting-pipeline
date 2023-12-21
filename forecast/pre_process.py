"""
Pipeline to process the input dataset and configuration files, preparing for the actual forecasting 
pipeline.
"""
from forecast.utils import S3Utility
from os import environ
from typing import Union
import pandas as pd

BUCKET = environ.get("FORECAST_S3_BUCKET")

class PreProcess(S3Utility):
	"""
	Process the CSV dataset into the shape required by the forecasting lambda
	"""
	def __init__(self, **kwargs):
		self.config = self.get_s3_object(bucket=bucket, key=kwargs.get("config_file"))
		self.dataset = None


	def run(self) -> PreProcess:
		"""
		Run the pipeline
		"""
		(
			self.load_dataset()
				.create_model_dataset()
		)

		return self

	def load_dataset(self) -> PreProcess:
		"""
		Read the S3 location of the CSV dataset and load it as a pandas dataframe
		"""
		self.dataset = {
			"meta_data": self.config["Dataset"]["meta_data"]
			"data": pd.read_csv(self.config["Dataset"]["file_name"])
		} 

		return self

	def create_model_dataset(self) -> PreProcess:
		"""
		Create individual model datasets
		"""
		model_dataset = {"config": self.config["Config"]}
		model_dataset.update(self.dataset)

		if "group_models_by" in model_dataset["meta_data"].keys():
			model_dataset["model_group"] = {}
			model_dataset = [self.split_model_dataset(model_dataset)]

	@staticmethod
	def slice_dataset(model: dict, groups: np.ndarray, column: str) -> dict:
		"""
		Split a dataset into individual dataframes 
		"""
		data = model["data"]
		return {group: data.loc[data[column] == group, data.columns != column] for group in groups}

	def split_model_dataset(self, model: dict) -> Union[list, dict]:
		"""

		"""
		columns = model["meta_data"]["group_models_by"].copy()
		if columns:
			group_by_col = columns.pop()
			groups = model["data"][group_by_col].unique()

			grouped_data = self.slice_dataset(model, groups, group_by_col)

			models = []
			for group in groups:
				model_group = model["model_group"].copy()
				model_group[group_by_col] = group
				model_tmp = {
					"config": model["config"],
					"model_group": model_group,
					"meta_data": model["meta_data"],
					"data": grouped_data[group]
				}
				models.append(model_tmp)

			return_model = [self.split_model_dataset(mod) for mod in models]
		else:
			return model = model

		return return_model



def pre_process_handler(event: Any, context: Any) -> str:
	"""
	Lambda handler to run the preprocess pipeline
	"""
	return PreProcess(config_file=event.get("config_file")).run()

