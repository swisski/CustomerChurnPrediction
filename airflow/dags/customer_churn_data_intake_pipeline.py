import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import sys
from pathlib import Path

# Add the plugins directory to the Python path
plugins_dir = Path(__file__).parents[1] / "plugins"
sys.path.append(str(plugins_dir))

# Now import your function
from helpers.augment_data import augment_data

# Get the password from the environment variable
db_password = os.environ.get('AIRFLOW_PASSWORD')

def ingest_data():
    df = pd.read_csv('/home/swisski/airflow/data/Updated_Churn_Data.csv')
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password=db_password,
        host="localhost"
    )
    cur = conn.cursor()
    
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO customer_data (
                customerID, gender, SeniorCitizen, Partner, Dependents,
                tenure, PhoneService, MultipleLines, InternetService,
                OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport,
                StreamingTV, StreamingMovies, Contract, PaperlessBilling,
                PaymentMethod, MonthlyCharges, TotalCharges, Churn, date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (customerID, date) DO UPDATE 
            SET 
                gender = EXCLUDED.gender,
                SeniorCitizen = EXCLUDED.SeniorCitizen,
                Partner = EXCLUDED.Partner,
                Dependents = EXCLUDED.Dependents,
                tenure = EXCLUDED.tenure,
                PhoneService = EXCLUDED.PhoneService,
                MultipleLines = EXCLUDED.MultipleLines,
                InternetService = EXCLUDED.InternetService,
                OnlineSecurity = EXCLUDED.OnlineSecurity,
                OnlineBackup = EXCLUDED.OnlineBackup,
                DeviceProtection = EXCLUDED.DeviceProtection,
                TechSupport = EXCLUDED.TechSupport,
                StreamingTV = EXCLUDED.StreamingTV,
                StreamingMovies = EXCLUDED.StreamingMovies,
                Contract = EXCLUDED.Contract,
                PaperlessBilling = EXCLUDED.PaperlessBilling,
                PaymentMethod = EXCLUDED.PaymentMethod,
                MonthlyCharges = EXCLUDED.MonthlyCharges,
                TotalCharges = EXCLUDED.TotalCharges,
                Churn = EXCLUDED.Churn
        """, (
            row['customerID'], row['gender'], row['SeniorCitizen'],
            row['Partner'], row['Dependents'], row['tenure'],
            row['PhoneService'], row['MultipleLines'], row['InternetService'],
            row['OnlineSecurity'], row['OnlineBackup'], row['DeviceProtection'],
            row['TechSupport'], row['StreamingTV'], row['StreamingMovies'],
            row['Contract'], row['PaperlessBilling'], row['PaymentMethod'],
            row['MonthlyCharges'], row['TotalCharges'], row['Churn'],
            row['date']
        ))
    
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'customer_churn_data_intake_pipeline',
    default_args=default_args,
    description='Augment and ingest customer data, then run transformations and predictions',
    schedule_interval=timedelta(days=1),
)

augment_data_task = PythonOperator(
    task_id='augment_data',
    python_callable=augment_data,
    op_kwargs={
        'input_file': '/home/swisski/airflow/data/Clean_Churn_Data.csv',
        'output_file': '/home/swisski/airflow/data/Updated_Churn_Data.csv'
    },
    dag=dag,
)

ingest_data_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

run_dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command='cd ~/dbt_projects/customer_churn && dbt run',
    dag=dag,
)


# Define the SQL for creating the customer_data table
create_table_sql = """
CREATE TABLE IF NOT EXISTS customer_data (
    customerID VARCHAR(255),
    gender VARCHAR(10),
    SeniorCitizen INTEGER,
    Partner VARCHAR(5),
    Dependents VARCHAR(5),
    tenure INTEGER,
    PhoneService VARCHAR(5),
    MultipleLines VARCHAR(20),
    InternetService VARCHAR(20),
    OnlineSecurity VARCHAR(5),
    OnlineBackup VARCHAR(5),
    DeviceProtection VARCHAR(5),
    TechSupport VARCHAR(5),
    StreamingTV VARCHAR(5),
    StreamingMovies VARCHAR(5),
    Contract VARCHAR(20),
    PaperlessBilling VARCHAR(5),
    PaymentMethod VARCHAR(255),
    MonthlyCharges NUMERIC,
    TotalCharges NUMERIC,
    Churn VARCHAR(5),
    date DATE,
    PRIMARY KEY (customerID, date)
);
"""

create_table_task = PostgresOperator(
    task_id='create_customer_data_table',
    postgres_conn_id='postgres_default',
    sql=create_table_sql,
    dag=dag,
)


# Define the task dependencies
create_table_task >> augment_data_task >> ingest_data_task >> run_dbt_task