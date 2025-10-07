import requests
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

import pickle
import os


def extract_data():
    """
    Loads data from web scrape request and saves as .csv file.
    Returns:
        str: output file path 
    """
    # Calculate dates for TWO years (roughly 720 days)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=720)).strftime('%Y-%m-%d')

    # Direct CSV download URL with date parameters
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE&cosd={start_date}&coed={end_date}"

    # Get the data
    response = requests.get(url)
    if response.status_code == 200:
        # Parse CSV data
        df = pd.read_csv(StringIO(response.text))

    #data_path = "../data/unemployment.csv"
    data_path = "/opt/airflow/dags/data/unemployment.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    df.to_csv(data_path, index=False)
    return data_path


def transform_data(ti):
    """
    Use Task Instance object (ti) to retrieve returned output path from extract_data(),
    and create training varialbles x and y. Save them as .csv
    Returns: list of output file path for x.csv and y.csv
    """

    data_path = ti.xcom_pull(task_ids='extract_data_task')
    df = pd.read_csv(data_path)

    # extract features
    y = df['UNRATE'].to_numpy()
    x = np.arange(len(y)).reshape(-1, 1)
    y = y.reshape(-1, 1)

    # save features
    out_paths = ["/opt/airflow/dags/data/x.csv", '/opt/airflow/dags/data/y.csv']
    os.makedirs('/opt/airflow/dags/data', exist_ok=True)

    np.savetxt(out_paths[0], x, delimiter=',')
    np.savetxt(out_paths[1], y, delimiter=',')
    return out_paths


def build_save_model(ti):
    """
    Builds a Linear Regression on the data and saves it.
    Returns: output directory for the model, directory for predictions, directory for labels
    """
    # get x(months) and y(unemployment rate) variables
    data_path = ti.xcom_pull(task_ids='transform_data_task')
    x = np.loadtxt(data_path[0], delimiter=',',dtype=float)
    y = np.loadtxt(data_path[1], delimiter=',',dtype=float)
    x = x.reshape(-1, 1)
    y = y.reshape(-1, 1)
    # train model
    lr = LinearRegression()
    lr.fit(x,y)

    # make and save predictions
    predict = lr.predict(x)
    predict_path= '/opt/airflow/dags/data/predict.csv'
    y_path = '/opt/airflow/dags/data/y.csv'
    np.savetxt(predict_path, predict, delimiter=',')

    output_dir = "/opt/airflow/dags/model"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'linear_model.pkl')
    with open(output_path, 'wb') as f:
        pickle.dump(lr, f)
    
    return [output_path, predict_path, y_path]


def model_eval(ti):
    """
    Loads the saved model and evaluates it.
    Return metrics: mse, r2 and prediction for next month unemployment as a list  
    """
    # list of two string, first is model path
    paths = ti.xcom_pull(task_ids='build_save_model_task')


    with open(paths[0], 'rb') as f:
        loaded_model = pickle.load(f)

    predict = np.loadtxt(paths[1], delimiter=',',dtype=float)
    true_unemployment = np.loadtxt(paths[2], delimiter=',', dtype=float)
    
    # evaluate model MSE:
    mse = float(mean_squared_error(true_unemployment, predict))
    
    # evaluate r2:
    r2 = float(r2_score(true_unemployment, predict))

    # predict next month unemployment (24th month is next month)
    next_month = float(loaded_model.predict([[24]])[0][0])
    return [mse, r2, next_month]

