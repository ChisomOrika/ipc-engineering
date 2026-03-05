{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key=['customer_id', 'branch_id', 'wallet_created_date']
) }}

SELECT
  c.customer_business_name AS customer_name,
  b.branch_name,
  w.wallet_customer_id_fk AS customer_id,
  w.wallet_branch_id_fk AS branch_id,
  w.wallet_is_active,
  w.wallet_created_at_date_time::date AS wallet_created_date,
  SUM(w.wallet_balance_amount) AS total_wallet_balance_amount,
  SUM(w.wallet_current_balance_amount) AS total_wallet_current_balance_amount,
  SUM(w.wallet_temp_balance_amount) AS total_wallet_temp_balance_amount
FROM {{ ref('bv_dash_wallets') }} w
LEFT JOIN {{ ref('bv_dash_customers') }} c ON w.wallet_customer_id_fk = c.customer_id_pk
LEFT JOIN {{ ref('bv_dash_branches') }} b ON w.wallet_branch_id_fk = b.branch_id_pk
{% if is_incremental() %}
  WHERE w.wallet_updated_at_date_time > (SELECT MAX(wallet_created_date) FROM {{ this }})
{% endif %}
GROUP BY
  customer_name,
  branch_name,
  customer_id,
  branch_id,
  wallet_is_active,
  wallet_created_date