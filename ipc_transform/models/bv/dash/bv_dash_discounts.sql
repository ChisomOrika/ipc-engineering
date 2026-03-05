{{ config(materialized='table', schema='bv', tags=['DAASH']) }}

with bv_discounts as (
  select
  "_id" as discount_id_pk,
  "title" as discount_title,
  "type" as discount_type,
  "target" as discount_target,
  "channel" as discount_channel,
  "category" as discount_category,
  "value" as discount_value,
  "startDate" as discount_start_date_time,
  "endDate" as discount_end_date_time,
  "isFirstTimeUserOnly" as discount_is_first_time_user_only,
  "minimumOrderAmount" as discount_minimum_order_amount,
  "minQuantityPerItem" as discount_minimum_quantity_per_item,
  "partialComboThreshold" as discount_partial_combo_threshold,
  "usageLimit" as discount_usage_limit,
  "usageCount" as discount_usage_count,
  "applicableItems" as discount_applicable_items,
  "comboItems" as discount_combo_items,
  "active" as discount_is_active,
  "partialComboAllowed" as discount_partial_combo_allowed,
  "customer" as discount_customer_id_fk,
  "createdAt" as discount_created_at_date_time,
  "updatedAt" as discount_updated_at_date_time,
  "__v" as discount___v,
  "code" as discount_code,
  "record_load_date" as discount_record_load_date
from {{ ref('raw_dash_discounts') }}

)

select *
from bv_discounts
