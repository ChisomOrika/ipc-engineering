
{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH customer_details AS (

SELECT customer_id_pk, 
       customer_businessname
FROM {{ ref('bv_customers') }} c  
)

SELECT * FROM customer_details