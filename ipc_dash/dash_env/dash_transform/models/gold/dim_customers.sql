{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH customer_details AS (

SELECT customer_id_pk,
  customer_business_name,
  customer_active,
  customer_business_type,
  customer_created_at_date_time,
  customer_updated_at_date_time
FROM {{ ref('bv_customers') }} c  
)

SELECT * FROM customer_details