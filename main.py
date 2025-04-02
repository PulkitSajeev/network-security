from NetSec.components.data_ingestion import DataIngestion
from NetSec.components.data_validation import DataValidation
from NetSec.components.data_transformation import DataTransformation
from NetSec.exception import CustomException
from NetSec.logger import logging
from NetSec.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig, DataValidationConfig, DataTransformationConfig
import sys

if __name__ == "__main__":
    try:
        '''
        Data Ingestion
        '''
        logging.info("Starting data ingestion")
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config)
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        logging.info("data initiation completed")
        print(data_ingestion_artifact)

        '''
        Data Validation
        '''
        data_validation_config = DataValidationConfig(training_pipeline_config)
        data_validation = DataValidation(data_validation_config, data_ingestion_artifact)
        logging.info("initiate data validation")
        data_validation_artifact = data_validation.initiate_data_validation()
        logging.info("data validation completed")
        print(data_validation_artifact)

        '''
        Data Transformation
        '''
        data_transformation_config = DataTransformationConfig(training_pipeline_config)
        data_transformation = DataTransformation(data_validation_artifact, data_transformation_config)
        logging.info("initiate data transformation")
        data_transformation_artifact = data_transformation.initiate_data_transformation()
        logging.info("data transformation completed")
        print(data_transformation_artifact)
    except Exception as e:
        raise CustomException(e, sys)
    