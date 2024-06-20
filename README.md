# List of USA Stadiums By Capacity ETL Pipeline

This repository contains an ETL (Extract, Transform, Load) pipeline for extracting stadium data from Wikipedia, transforming it, and loading it into Azure Blob Storage. The pipeline is implemented using Apache Airflow and Python.
https://en.wikipedia.org/wiki/List_of_U.S._stadiums_by_capacity

## Table of Contents

- [Requirements](#requirements)
- [Setup](#setup)
- [Usage](#usage)
- [Airflow DAG](#airflow-dag)
- [Pipeline Code](#pipeline-code)
- [License](#license)

## Requirements

- Python 3.8+
- Apache Airflow 2.x
- Azure Blob Storage account
- `pip` for installing dependencies

## Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/Mims20/StadiumsInUSA.git
   cd StadiumsInUSA

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
 
4. **Set up environment variables:**
   
   Create a .env file in the project root and add the following variables:
   ```.env
   URL=https://en.wikipedia.org/wiki/List_of_U.S._stadiums_by_capacity
   ACCOUNT_KEY=your_azure_blob_storage_account_key
 

## Usage

### Running the Airflow DAG

1. **Initialize the Airflow Database:**
   
   Before running the Airflow DAG, initialize the Airflow database if you haven't already done so:
   ```bash
   airflow db init

2. **Start the Airflow Webserver:**
   ```bash
   airflow webserver

3. **Start the Airflow Scheduler:**
   ```bash
   airflow scheduler

4. **Access the Airflow web UI:**

   Open your browser and go to http://localhost:8080. You should see the DAG named Wikipedia_flow. Trigger it to start the ETL process.
 

## Airflow DAG

The Airflow DAG (dags.py) orchestrates the ETL process. It contains three main tasks:

Extract Task: Extracts data from the Wikipedia page.

Transform Task: Transforms the extracted data, including geolocation and data cleaning.

Load Task: Loads the transformed data into Azure Blob Storage.

## Pipeline Code
The pipeline code is located in pipelines/pipeline.py. It contains the functions for extracting, transforming, and loading the data.
