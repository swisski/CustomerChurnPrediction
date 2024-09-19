{{ config(materialized='table') }}

SELECT
    customerid,
    CASE WHEN gender = 'Male' THEN 1 ELSE 0 END as gender_male,
    seniorcitizen,
    CASE WHEN partner = 'Yes' THEN 1 ELSE 0 END as has_partner,
    CASE WHEN dependents = 'Yes' THEN 1 ELSE 0 END as has_dependents,
    tenure,
    CASE WHEN phoneservice = 'Yes' THEN 1 ELSE 0 END as has_phone_service,
    CASE WHEN multiplelines = 'Yes' THEN 1 ELSE 0 END as has_multiple_lines,
    CASE
        WHEN internetservice = 'Fiber optic' THEN 2
        WHEN internetservice = 'DSL' THEN 1
        ELSE 0
    END as internet_service_type,
    CASE WHEN onlinesecurity = 'Yes' THEN 1 ELSE 0 END as has_online_security,
    CASE WHEN onlinebackup = 'Yes' THEN 1 ELSE 0 END as has_online_backup,
    CASE WHEN deviceprotection = 'Yes' THEN 1 ELSE 0 END as has_device_protection,
    CASE WHEN techsupport = 'Yes' THEN 1 ELSE 0 END as has_tech_support,
    CASE WHEN streamingtv = 'Yes' THEN 1 ELSE 0 END as has_streaming_tv,
    CASE WHEN streamingmovies = 'Yes' THEN 1 ELSE 0 END as has_streaming_movies,
    CASE
        WHEN contract = 'Two year' THEN 2
        WHEN contract = 'One year' THEN 1
        ELSE 0
    END as contract_type,
    CASE WHEN paperlessbilling = 'Yes' THEN 1 ELSE 0 END as has_paperless_billing,
    CASE
        WHEN paymentmethod = 'Electronic check' THEN 0
        WHEN paymentmethod = 'Mailed check' THEN 1
        WHEN paymentmethod = 'Bank transfer (automatic)' THEN 2
        WHEN paymentmethod = 'Credit card (automatic)' THEN 3
    END as payment_method_type,
    monthlycharges,
    totalcharges,
    CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END as churn
FROM {{ ref('stg_customer_data') }}