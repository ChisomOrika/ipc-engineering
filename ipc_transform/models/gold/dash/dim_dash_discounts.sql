
{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key='discount_id_pk'
) }}

SELECT
  d.discount_id_pk,
  d.discount_title,
  d.discount_type,
  d.discount_target,
  d.discount_channel,
  d.discount_category,
  d.discount_value,
  d.discount_start_date_time,
  d.discount_end_date_time,
  d.discount_usage_limit,
  d.discount_usage_count,
  d.discount_updated_at_date_time as discount_updated_at,
  (d.discount_value * d.discount_usage_count) AS total_discount_value,
  TO_CHAR(d.discount_start_date_time, 'Mon FMDD') || ' - ' || TO_CHAR(d.discount_end_date_time, 'Mon FMDD') AS discount_date_range
FROM {{ ref('bv_dash_discounts') }} d

{% if is_incremental() %}
  WHERE d.discount_updated_at_date_time > (SELECT MAX(discount_updated_at)  FROM {{ this }})
{% endif %}
