import sys
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Add the parent directory to the system path for module imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import functions from the pipeline module
from pipelines.pipeline import extract_wikipedia_data, transform_wikipedia_data, load_wikipedia_data

# Load environment variables from a .env file
load_dotenv()

# Get the Wikipedia URL from environment variables
URL = os.getenv("URL")

# Default arguments for the DAG
default_args = {
    "owner": "Selase",
    "start_date": pendulum.today("utc")  # Set the start date to today in UTC
}

# Define the DAG with a daily schedule and no catchup
dag = DAG(
    dag_id="Wikipedia_flow",
    default_args=default_args,
    schedule="@daily",
    catchup=False
)

# Define the task for extracting Wikipedia data
extract_wikipedia_data = PythonOperator(
    task_id="extract_wikipedia_data",
    python_callable=extract_wikipedia_data,
    op_kwargs={"url": URL},
    provide_context=True,
    dag=dag
)

# Define the task for transforming the extracted data
transform_wikipedia_data = PythonOperator(
    task_id="transform_wikipedia_data",
    python_callable=transform_wikipedia_data,
    provide_context=True,
    dag=dag
)

# Define the task for loading the transformed data
load_wikipedia_data = PythonOperator(
    task_id="load_wikipedia_data",
    python_callable=load_wikipedia_data,
    provide_context=True,
    dag=dag
)

# Set the task dependencies to ensure sequential execution
extract_wikipedia_data >> transform_wikipedia_data >> load_wikipedia_data
