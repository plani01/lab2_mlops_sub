# Import necessary libraries and modules
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime, timedelta
from src.lab import extract_data, transform_data, build_save_model, model_eval

#from airflow.operators.bash import BashOperator

# NOTE:
# In Airflow 3.x, enabling XCom pickling should be done via environment variable:
# export AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
# The old airflow.configuration API is deprecated.

# Define default arguments for your DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2025, 1, 15),
    'retries': 0,  # Number of retries in case of task failure
    'retry_delay': timedelta(minutes=5),  # Delay before retries
}

# Create a DAG instance named 'Airflow_Lab1' with the defined default arguments
with DAG(
    'Airflow_Lab1',
    default_args=default_args,
    description='Dag Modification for Lab 1 (lab2 submission) of Airflow series',
    catchup=False,
) as dag:

    # Task to load data, calls the 'load_data' Python function
    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data,
    )

    # Task to perform data preprocessing, depends on 'load_data_task'
    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data,
    )

    # Task to build and save a model, depends on 'data_preprocessing_task'
    build_save_model_task = PythonOperator(
        task_id='build_save_model_task',
        python_callable=build_save_model,
    )

    # Task to load a model using the 'load_model_elbow' function, depends on 'build_save_model_task'
    model_eval_task = PythonOperator(
        task_id='model_eval_task',
        python_callable=model_eval,
    )

    # Set task dependencies
    extract_data_task >> transform_data_task >> build_save_model_task >> model_eval_task

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.test()
