from NetSec.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from NetSec.entity.config_entity import DataValidationConfig
from NetSec.exception import CustomException
from NetSec.logger import logging
from scipy.stats import ks_2samp
import pandas as pd
import os
import sys
from NetSec.constant.training_pipeline import SCHEMA_FILE_PATH
from NetSec.utils.main_utils.utils import read_yaml_file, write_yaml_file

class DataValidation:
    def __init__(self, data_validation_config: DataValidationConfig, data_ingestion_artifact: DataIngestionArtifact):
        try:
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.schema_config= read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise CustomException(e, sys)
    
    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            raise CustomException(e, sys)

    def validate_num_columns(self, df: pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self.schema_config)
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Dataframe has columns: {len(df.columns)}")
            if len(df.columns) != number_of_columns:
                return True
            return False
        except Exception as e:
            raise CustomException(e, sys)
    
    def detect_dataset_drift(self, base_df, current_df, threshold = 0.05) -> bool:
        try:
            drift_report = {}
            for column in base_df.columns:
                if column != 'Result':  # Assuming 'Result' is the target variable
                    base_data = base_df[column]
                    current_data = current_df[column]
                    ks_statistic, p_value = ks_2samp(base_data, current_data)
                    if p_value < threshold:
                        drift_report[column] = {
                            "ks_statistic": ks_statistic,
                            "p_value": p_value
                        }
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok = True)
            write_yaml_file(file_path = drift_report_file_path, content = drift_report)
            return len(drift_report) > 0
        except Exception as e:
            raise CustomException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            #read the data from train and test
            train_df = DataValidation.read_data(train_file_path)
            test_df = DataValidation.read_data(test_file_path)

            #validate the number of columns
            status = self.validate_num_columns(df = train_df)
            if not status:
                error_message = f"Number of columns in the train file is not as per the schema. Expected: {len(self.schema_config)}, Found: {len(train_df.columns)}"
            status = self.validate_num_columns(df = test_df)
            if not status:
                error_message = f"Number of columns in the test file is not as per the schema. Expected: {len(self.schema_config)}, Found: {len(test_df.columns)}"
                
            #check datadrift
            status = self.detect_dataset_drift(base_df = train_df, current_df = test_df)
            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok = True)
            train_df.to_csv(self.data_validation_config.valid_train_file_path, index = False, header = True)
            test_df.to_csv(self.data_validation_config.valid_test_file_path, index = False, header = True)
            
            data_validation_artifact = DataValidationArtifact(
                validation_status = status,
                valid_train_file_path = self.data_ingestion_artifact.train_file_path,
                valid_test_file_path = self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path = None,
                invalid_test_file_path = None,
                drift_report_file_path = self.data_validation_config.drift_report_file_path
            )

            return data_validation_artifact

        except Exception as e:
            raise CustomException(e, sys)