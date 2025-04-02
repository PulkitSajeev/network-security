import yaml
import os
import sys
import numpy as np
import pickle
from NetSec.exception import CustomException
from NetSec.logger import logging

def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path, 'r') as file:
            content = yaml.safe_load(file)
        return content
    except Exception as e:
        raise CustomException(e, sys)

def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok = True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise CustomException(e, sys)

def save_numpy_array_data(file_path: str, array: np.array) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok = True)
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
        logging.info("saved numpy array data")
    except Exception as e:
        raise CustomException(e, sys)

def save_object(file_path: str, obj: object) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok = True)
        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)
        logging.info("saved object")
    except Exception as e:
        raise CustomException(e, sys)