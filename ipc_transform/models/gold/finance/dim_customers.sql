{{ config(materialized='table', schema='gold', tags=['Finance', 'Customers']) }}

-- Unified customer dimension: DAASH + GoSource
-- service_line tag identifies which platform the customer belongs to
-- Customer IDs are MongoDB ObjectIDs from separate DBs — no collision risk

with daash_customers as (
    select
        customer_id_pk,
        coalesce(
            nullif(trim(customer_business_name), ''),
            nullif(trim(concat(customer_first_name, ' ', customer_last_name)), '')
        )                                                                   as customer_name,
        customer_email,
        customer_phone_number                                               as customer_phone,
        customer_business_type,
        customer_active::text,
        customer_created_at_date_time::date                                 as customer_created_date,
        'DAASH'                                                             as service_line
    from {{ ref('bv_dash_customers') }}
),

gosource_customers as (
    select
        customer_id_pk,
        customer_business_name                                              as customer_name,
        null::text                                                          as customer_email,
        null::text                                                          as customer_phone,
        null::text                                                          as customer_business_type,
        customer_verified::text                                              as customer_active,
        null::date                                                          as customer_created_date,
        'GoSource'                                                          as service_line
    from {{ ref('bv_gosource_customers') }}
)

select
    customer_id_pk                                                          as customer_id_pk,
    customer_name,
    customer_email,
    customer_phone,
    customer_business_type,
    customer_active,
    customer_created_date,
    service_line
from daash_customers

union all

select
    customer_id_pk,
    customer_name,
    customer_email,
    customer_phone,
    customer_business_type,
    customer_active,
    customer_created_date,
    service_line
from gosource_customers
