{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key='customer_id_pk'
) }}

WITH customer_details AS (

SELECT
  customer_id_pk,
  customer_business_name,
  customer_active,
  customer_business_type,
  customer_created_at_date_time,
  customer_updated_at_date_time
FROM {{ ref('bv_dash_customers') }} c

{% if is_incremental() %}
  WHERE customer_updated_at_date_time > (SELECT MAX(customer_updated_at_date_time) FROM {{ this }})
{% endif %}

)

SELECT * FROM customer_details