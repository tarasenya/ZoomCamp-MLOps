import mlflow
import os
# Q1. Install the package. What's the version that you have?
print(mlflow.__version__)
# Q2 Execute python preprocess_data.py --raw_data_path <TAXI_DATA_FOLDER> --dest_path ./output. What is the size of
# DictVectorizer?
TAXI_DATA_FOLDER = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), 'data')
#os.system(f"python preprocess_data.py --raw_data_path {TAXI_DATA_FOLDER} --dest_path ./output")
DICT_VECTORIZER_PATH = os.path.join(os.getcwd(),'output/dv.pkl')
print(f"Size equals {os.path.getsize(DICT_VECTORIZER_PATH)/1024}")

