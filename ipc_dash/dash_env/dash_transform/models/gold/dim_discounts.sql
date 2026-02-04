{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH discount_details AS (

SELECT
  discounts_id_pk,
  discounts_title,
  discounts_type,
  discounts_target,
  discounts_channel,
  discounts_category,
  discounts_value,
  discounts_start_date_time,
  discounts_end_date_time,
  discounts_usage_limit,
  discounts_usage_count,
  (discounts_value * discounts_usage_count) AS total_discount_value,
  TO_CHAR(discounts_start_date_time, 'Mon FMDD') || ' - ' || TO_CHAR(discounts_end_date_time, 'Mon FMDD') AS discount_date_range
FROM {{ ref('bv_discounts') }} 
)

SELECT * FROM discount_details