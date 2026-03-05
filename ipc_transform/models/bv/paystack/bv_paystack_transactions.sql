{{ config(materialized='table', schema='bv', tags=['Paystack']) }}

with bv_transactions as (
    select
        -- identifiers
        id                                                                          as transaction_id_pk,
        "domain"                                                                    as transaction_domain,
        status                                                                      as transaction_status,
        reference                                                                   as transaction_reference,
        order_id                                                                    as transaction_order_id,

        -- amounts (converted from kobo to naira)
        amount                                                                      as transaction_amount_kobo,
        (amount::numeric / 100)                                                     as transaction_amount,
        currency                                                                    as transaction_currency,
        fees                                                                        as transaction_fees_kobo,
        (fees::numeric / 100)                                                       as transaction_fees_amount,
        requested_amount                                                            as transaction_requested_amount_kobo,
        (requested_amount::numeric / 100)                                           as transaction_requested_amount,

        -- payment details
        channel                                                                     as transaction_channel,
        message                                                                     as transaction_message,
        gateway_response                                                            as transaction_gateway_response,
        ip_address                                                                  as transaction_ip_address,
        plan                                                                        as transaction_plan,
        split                                                                       as transaction_split,
        subaccount                                                                  as transaction_subaccount,
        fees_split                                                                  as transaction_fees_split,
        "connect"                                                                   as transaction_connect,
        pos_transaction_data                                                        as transaction_pos_transaction_data,

        -- customer (extracted from JSON)
        NULLIF(customer, '')::jsonb ->> 'id'                                        as customer_paystack_id,
        NULLIF(customer, '')::jsonb ->> 'email'                                     as customer_email,
        NULLIF(customer, '')::jsonb ->> 'first_name'                                as customer_first_name,
        NULLIF(customer, '')::jsonb ->> 'last_name'                                 as customer_last_name,
        NULLIF(customer, '')::jsonb ->> 'phone'                                     as customer_phone,
        NULLIF(customer, '')::jsonb ->> 'customer_code'                             as customer_code,
        NULLIF(customer, '')::jsonb ->> 'risk_action'                               as customer_risk_action,

        -- authorization / card details (extracted from JSON)
        NULLIF("authorization", '')::jsonb ->> 'authorization_code'                as authorization_code,
        NULLIF("authorization", '')::jsonb ->> 'bin'                               as card_bin,
        NULLIF("authorization", '')::jsonb ->> 'last4'                             as card_last4,
        NULLIF("authorization", '')::jsonb ->> 'exp_month'                         as card_exp_month,
        NULLIF("authorization", '')::jsonb ->> 'exp_year'                          as card_exp_year,
        NULLIF("authorization", '')::jsonb ->> 'card_type'                         as card_type,
        NULLIF("authorization", '')::jsonb ->> 'bank'                              as card_bank,
        NULLIF("authorization", '')::jsonb ->> 'country_code'                      as card_country_code,
        NULLIF("authorization", '')::jsonb ->> 'brand'                             as card_brand,
        NULLIF("authorization", '')::jsonb ->> 'reusable'                          as card_reusable,
        NULLIF("authorization", '')::jsonb ->> 'account_name'                      as card_account_name,

        -- source (extracted from JSON)
        NULLIF("source", '')::jsonb ->> 'source'                                   as payment_source,
        NULLIF("source", '')::jsonb ->> 'type'                                     as payment_source_type,
        NULLIF("source", '')::jsonb ->> 'entry_point'                              as payment_source_entry_point,

        -- metadata: order context (extracted from JSON)
        NULLIF(metadata, '')::jsonb ->> 'userId'                                   as metadata_user_id,
        NULLIF(metadata, '')::jsonb ->> 'paymentReference'                         as metadata_payment_reference,
        NULLIF(metadata, '')::jsonb -> 'order' ->> 'branch'                        as metadata_order_branch_id,
        NULLIF(metadata, '')::jsonb -> 'order' ->> 'customer'                      as metadata_order_customer_id,
        NULLIF(metadata, '')::jsonb -> 'order' ->> 'paymentMethod'                 as metadata_payment_method,
        NULLIF(metadata, '')::jsonb -> 'order' ->> 'deliveryType'                  as metadata_delivery_type,
        NULLIF(metadata, '')::jsonb -> 'order' ->> 'orderType'                     as metadata_order_type,
        NULLIF(metadata, '')::jsonb -> 'order' ->> 'channel'                       as metadata_order_channel,
        NULLIF(metadata, '')::jsonb -> 'order' ->> 'phoneNumber'                   as metadata_customer_phone,
        NULLIF(metadata, '')::jsonb -> 'order' -> 'address' ->> 'streetAddress'    as metadata_delivery_street_address,
        NULLIF(metadata, '')::jsonb -> 'order' -> 'address' ->> 'state'            as metadata_delivery_state,
        NULLIF(metadata, '')::jsonb -> 'order' -> 'address' ->> 'lga'              as metadata_delivery_lga,
        NULLIF(metadata, '')::jsonb -> 'order' -> 'scheduleDetails' ->> 'date'     as metadata_schedule_date,
        NULLIF(metadata, '')::jsonb -> 'order' -> 'scheduleDetails' ->> 'time'     as metadata_schedule_time,
        NULLIF(metadata, '')::jsonb ->> 'referrer'                                 as metadata_referrer,

        -- metadata products array retained as JSON (array — use gold layer to unnest)
        NULLIF(metadata, '')::jsonb -> 'order' -> 'products'                       as metadata_order_products,

        -- timestamps
        paid_at                                                                     as transaction_paid_at_date_time,
        created_at                                                                  as transaction_created_at_date_time,
        record_load_date                                                            as transaction_record_load_date

    from {{ ref('raw_paystack_transactions') }}
)

select * from bv_transactions
