
{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key='revenue_ledgers_created_at'
) }}

SELECT
  revenue_ledgers_created_at_date_time::date AS revenue_ledgers_created_at,
  SUM(revenue_ledgers_amount) AS revenue_ledgers_amount
FROM {{ ref('bv_dash_revenueledgers') }}
{% if is_incremental() %}
  WHERE revenue_ledgers_updated_at_date_time > (SELECT MAX(revenue_ledgers_created_at) FROM {{ this }})
{% endif %}
GROUP BY revenue_ledgers_created_at
