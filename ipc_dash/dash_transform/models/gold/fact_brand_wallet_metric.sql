{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH wallet_details AS (

  SELECT
    c.customer_business_name AS customer_name,
    b.branches_name AS branch_name,
    w.wallet_customer_id_fk as customer_id,
    w.wallet_branch_id_fk as branch_id,
    w.wallet_is_active,
    w.wallet_created_at_date_time::date as wallet_created_date,
    sum(w.wallet_balance_amount) as total_wallet_balance_amount,
    sum(w.wallet_current_balance_amount) as total_wallet_current_balance_amount,
    sum(w.wallet_temp_balance_amount) as total_wallet_temp_balance_amount

  FROM {{ ref('bv_wallets') }} w
  LEFT JOIN {{ ref('bv_customers') }} c ON w.wallet_customer_id_fk = c.customer_id_pk
  LEFT JOIN {{ ref('bv_branches') }} b ON w.wallet_branch_id_fk = b.branches_id_pk
  GROUP BY 
  customer_name,
  branch_name,
  customer_id,
  branch_id,
  wallet_is_active,
  wallet_created_date
)

SELECT * FROM wallet_details









