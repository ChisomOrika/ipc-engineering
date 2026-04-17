{{ config(materialized='table', schema='gold', tags=['Finance']) }}

with ipc_accounts as (
    select
        account_id_pk,
        account_name,
        account_currency,
        account_type,
        account_status,
        account_available_balance_amount,
        account_current_balance_amount,
        account_bank_account,
        account_created_at_date_time,
        'IPC'           as business_unit,
        case
            when account_name ilike '%admin%'       then 'Admin'
            when account_name ilike '%purchasing%'  then 'Purchasing'
            when account_name ilike '%payment%'     then 'Payments'
            when account_name ilike '%marketing%'   then 'Marketing'
            when account_name ilike '%management%'  then 'Management'
            else 'General'
        end as account_purpose
    from {{ ref('bv_lenco_accounts') }}
),

gosource_accounts as (
    select
        account_id_pk,
        account_name,
        account_currency,
        account_type,
        account_status,
        account_available_balance_amount,
        account_current_balance_amount,
        account_bank_account,
        account_created_at_date_time,
        'GoSource'      as business_unit,
        case
            when account_name ilike '%services%'        then 'Main'
            when account_name ilike '%admin%'            then 'Admin'
            when account_name ilike '%procurement one%'  then 'Procurement 1'
            when account_name ilike '%procurement two%'  then 'Procurement 2'
            when account_name ilike '%frozen%'           then 'Frozen Foods'
            when account_name ilike '%service pay%'      then 'Service Payments'
            else 'General'
        end as account_purpose
    from {{ ref('bv_gosource_lenco_accounts') }}
)

select * from ipc_accounts
union all
select * from gosource_accounts
