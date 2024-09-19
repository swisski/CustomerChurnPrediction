from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import torch
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from ml_ops import AdvancedChurnPredictor, cross_validate, train_final_model, evaluate_model, save_model, load_model

def prepare_data(**kwargs):
    # Load data from PostgreSQL
    df = pd.read_sql("SELECT * FROM ml_features", engine)
    
    # Split data
    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
    
    # Save splits
    train_df.to_csv('/path/to/train_data.csv', index=False)
    test_df.to_csv('/path/to/test_data.csv', index=False)

def train_and_validate(**kwargs):
    train_df = pd.read_csv('/path/to/train_data.csv')
    X = train_df.drop(['customerid', 'churn'], axis=1)
    y = train_df['churn']
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    X_tensor = torch.FloatTensor(X_scaled)
    y_tensor = torch.FloatTensor(y.values)
    
    cv_results = cross_validate(X_tensor, y_tensor)
    print("Cross-validation results:", cv_results)
    
    final_model = train_final_model(X_tensor, y_tensor)
    save_model(final_model, '/path/to/final_model.pth')
    
    # Save scaler for later use
    joblib.dump(scaler, '/path/to/scaler.joblib')

def test_model(**kwargs):
    test_df = pd.read_csv('/path/to/test_data.csv')
    X_test = test_df.drop(['customerid', 'churn'], axis=1)
    y_test = test_df['churn']
    
    scaler = joblib.load('/path/to/scaler.joblib')
    X_test_scaled = scaler.transform(X_test)
    
    X_test_tensor = torch.FloatTensor(X_test_scaled)
    y_test_tensor = torch.FloatTensor(y_test.values)
    
    model = load_model('/path/to/final_model.pth', X_test.shape[1])
    test_results = evaluate_model(model, X_test_tensor, y_test_tensor)
    print("Test results:", test_results)

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
    'customer_churn_ml_pipeline',
    default_args=default_args,
    description='ML pipeline for customer churn prediction',
    schedule_interval=timedelta(days=1),
)

prepare_data_task = PythonOperator(
    task_id='prepare_data',
    python_callable=prepare_data,
    dag=dag,
)

train_and_validate_task = PythonOperator(
    task_id='train_and_validate',
    python_callable=train_and_validate,
    dag=dag,
)

test_model_task = PythonOperator(
    task_id='test_model',
    python_callable=test_model,
    dag=dag,
)

prepare_data_task >> train_and_validate_task >> test_model_task