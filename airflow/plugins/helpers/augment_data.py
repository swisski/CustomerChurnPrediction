import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def augment_data(input_file, output_file):
    # Read existing data
    df = pd.read_csv(input_file)
    
    # Add a date column if it doesn't exist
    if 'date' not in df.columns:
        df['date'] = datetime.now().date() - timedelta(days=len(df))
    
    # Get the last date in the dataset
    last_date = pd.to_datetime(df['date'].max())
    
    # Generate new dates
    new_dates = [last_date + timedelta(days=i+1) for i in range(7)]  # Add a week of new data
    
    # Create new rows
    new_rows = []
    for date in new_dates:
        for _ in range(5):  # Add 5 new customers per day
            new_customer = df.sample(n=1).iloc[0].copy()
            new_customer['customerID'] = f"NEW-{np.random.randint(1000, 9999)}"
            new_customer['date'] = date
            new_customer['tenure'] = np.random.randint(0, 72)
            
            # Randomize numeric columns
            new_customer['MonthlyCharges'] *= np.random.uniform(0.9, 1.1)
            new_customer['TotalCharges'] = new_customer['MonthlyCharges'] * new_customer['tenure']
            
            # Randomize categorical columns
            for col in ['InternetService', 'OnlineSecurity', 'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract', 'PaymentMethod']:
                new_customer[col] = np.random.choice(df[col].unique())
            
            # Adjust churn based on tenure and charges
            churn_probability = 0.1 - 0.001 * new_customer['tenure'] + 0.001 * new_customer['MonthlyCharges']
            new_customer['Churn'] = 'Yes' if np.random.random() < churn_probability else 'No'
            
            new_rows.append(new_customer)
    
    # Append new rows to the dataframe
    df_new = pd.DataFrame(new_rows)
    df_combined = pd.concat([df, df_new], ignore_index=True)
    
    # Save the combined dataframe
    df_combined.to_csv(output_file, index=False, mode='w')
    print(f"Augmented data saved to {output_file}")

if __name__ == "__main__":
    input_file = '/home/swisski/airflow/data/Clean_Churn_Data.csv'
    output_file = '/home/swisski/airflow/data/Updated_Churn_Data.csv'
    augment_data(input_file, output_file)