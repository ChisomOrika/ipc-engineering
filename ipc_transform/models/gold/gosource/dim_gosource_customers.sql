
{{ config(materialized='table', schema='gold', tags=['GoSource']) }}



WITH customer_details AS (

SELECT customer_id_pk,
       customer_business_name
FROM {{ ref('bv_gosource_customers') }} c
)

SELECT * FROM customer_details