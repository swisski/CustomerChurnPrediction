{{ config(materialized='table') }}

SELECT
    customerid,
    gender,
    seniorcitizen,
    partner,
    dependents,
    tenure,
    contract,
    monthlycharges,
    totalcharges,
    churn,
    CASE WHEN internetservice != 'No' THEN 1 ELSE 0 END as has_internet,
    CASE WHEN phoneservice = 'Yes' THEN 1 ELSE 0 END as has_phone,
    CASE 
        WHEN contract = 'Month-to-month' THEN 0
        WHEN contract = 'One year' THEN 1
        WHEN contract = 'Two year' THEN 2
    END as contract_duration
FROM {{ ref('stg_customer_data') }}