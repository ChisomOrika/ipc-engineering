{{ config(materialized='table', schema='bv', tags=['Python_Gosource']) }}

with bv_discounts as (
  select
  "_id" as discounts_id_pk,
  "title" as discounts_title,
  "type" as discounts_type,
  "target" as discounts_target,
  "channel" as discounts_channel,
  "category" as discounts_category,
  "value" as discounts_value,
  "startDate" as discounts_start_date_time,
  "endDate" as discounts_end_date_time,
  "isFirstTimeUserOnly" as discounts_is_first_time_user_only,
  "minimumOrderAmount" as discounts_minimum_order_amount,
  "minQuantityPerItem" as discounts_min_quantity_per_item,
  "partialComboThreshold" as discounts_partial_combo_threshold,
  "usageLimit" as discounts_usage_limit,
  "usageCount" as discounts_usage_count,
  "applicableItems" as discounts_applicable_items,
  "comboItems" as discounts_combo_items,
  "active" as discounts_active,
  "partialComboAllowed" as discounts_partial_combo_allowed,
  "customer" as discounts_customer_id_fk,
  "createdAt" as discounts_created_at_date_time,
  "updatedAt" as discounts_updated_at_date_time,
  "__v" as discounts___v,
  "code" as discounts_code,
  "record_load_date" as discounts_record_load_date
from {{ ref('raw_discounts') }}

)

select *
from bv_discounts
