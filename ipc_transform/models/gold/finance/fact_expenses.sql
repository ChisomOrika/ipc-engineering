{{ config(materialized='table', schema='gold', tags=['Finance', 'Expenses']) }}

-- Unified expenses from IPC Lenco + GoSource Lenco debit transactions
-- 9japay debits excluded: they are internal sweep transfers, not real expenses
with ipc_debits as (
    select
        transaction_id_pk,
        transaction_narration,
        transaction_category,
        transaction_amount,
        transaction_fee_amount,
        transaction_reference,
        transaction_account_id_fk                      as transaction_account_id,
        transaction_completed_at_date_time::date       as transaction_date,
        'IPC'                                          as business_unit
    from {{ ref('bv_lenco_transactions') }}
    where transaction_type   = 'debit'
      and transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
),

gosource_debits as (
    select
        transaction_id_pk,
        transaction_narration,
        -- Categorize GoSource narrations via SQL patterns
        case
            when transaction_narration ilike '%stamp duty%'
              or transaction_narration ilike '%sms charge%'
              or transaction_narration ilike '%transfer levy%'
              or transaction_narration ilike '%vat%'
              or transaction_narration ilike '%charge from%'
                                                               then 'Bank Charges'
            when transaction_narration ilike '%salary%'
              or transaction_narration ilike '%salaries%'
                                                               then 'Salaries'
            when transaction_narration ilike '%rent%'           then 'Rent'
            when transaction_narration ilike '%server expense%'
              or transaction_narration ilike '%software%'
              or transaction_narration ilike '%subscription%'
                                                               then 'Tech & Software'
            when transaction_narration ilike '%chicken%'
              or transaction_narration ilike '%fries%'
              or transaction_narration ilike '%french fries%'
              or transaction_narration ilike '%meat%'
              or transaction_narration ilike '%rice%'
              or transaction_narration ilike '%turkey%'
              or transaction_narration ilike '%flour%'
              or transaction_narration ilike '%sausage%'
              or transaction_narration ilike '%beef%'
              or transaction_narration ilike '%pepper%'
              or transaction_narration ilike '%oil%'
              or transaction_narration ilike '%sugar%'
              or transaction_narration ilike '%bread%'
              or transaction_narration ilike '%bacon%'
              or transaction_narration ilike '%cheese%'
              or transaction_narration ilike '%procurement%'
              or transaction_narration ilike '%supply%'
              or transaction_narration ilike '%perishable%'
              or transaction_narration ilike '%store items%'
              or transaction_narration ilike '%purchase%'
              or transaction_narration ilike '%#PO-%'
              or transaction_narration ilike '%PO-%'
              or transaction_narration ilike '%part payment for%'
              or transaction_narration ilike '%bal payment%'
              or transaction_narration ilike '%balance payment%'
              or transaction_narration ilike '%frozen food%'
              or transaction_narration ilike '%ketchup%'
              or transaction_narration ilike '%soy sauce%'
              or transaction_narration ilike '%honey%'
              or transaction_narration ilike '%sweetcorn%'
              or transaction_narration ilike '%sweet corn%'
              or transaction_narration ilike '%sweet chili%'
              or transaction_narration ilike '%sweet chilli%'
              or transaction_narration ilike '%paprika%'
              or transaction_narration ilike '%vinegar%'
              or transaction_narration ilike '%maggi%'
              or transaction_narration ilike '%yeast%'
              or transaction_narration ilike '%mustard%'
              or transaction_narration ilike '%butter%'
              or transaction_narration ilike '%jago%'
              or transaction_narration ilike '%curry%'
              or transaction_narration ilike '%sauce%'
              or transaction_narration ilike '%nylon%'
              or transaction_narration ilike '%pouch pack%'
              or transaction_narration ilike '%sauce cup%'
              or transaction_narration ilike '%cling film%'
              or transaction_narration ilike '%gloves%'
              or transaction_narration ilike '%sandwich pack%'
              or transaction_narration ilike '%hot dog pack%'
              or transaction_narration ilike '%hot pack%'
              or transaction_narration ilike '%plastic fork%'
              or transaction_narration ilike '%packaging%'
              or transaction_narration ilike '%drinks%'
              or transaction_narration ilike '%coconut powder%'
              or transaction_narration ilike '%jubi%'
              or transaction_narration ilike '%milk flavor%'
              or transaction_narration ilike '%ambassador%'
              or transaction_narration ilike '%dependox%'
              or transaction_narration ilike '%basmati%'
              or transaction_narration ilike '%sriracha%'
              or transaction_narration ilike '%dijon%'
              or transaction_narration ilike '%kraft%'
              or transaction_narration ilike '%burger%'
              or transaction_narration ilike '%serviette%'
              or transaction_narration ilike '%dano milk%'
              or transaction_narration ilike '%green peas%'
              or transaction_narration ilike '%tomatoes%'
              or transaction_narration ilike '%tomato paste%'
              or transaction_narration ilike '%cooking cream%'
              or transaction_narration ilike '%danica%'
              or transaction_narration ilike '%cayenne%'
              or transaction_narration ilike '%basil%'
              or transaction_narration ilike '%gino%'
              or transaction_narration ilike '%spaghetti%'
              or transaction_narration ilike '%blue band%'
              or transaction_narration ilike '%garlic%'
              or transaction_narration ilike '%ginger%'
              or transaction_narration ilike '%jollof%'
              or transaction_narration ilike '%takeaway%'
              or transaction_narration ilike '%take away%'
              or transaction_narration ilike '%carla towel%'
              or transaction_narration ilike '%mama lemon%'
              or transaction_narration ilike '%morning fresh%'
              or transaction_narration ilike '%hypo%'
              or transaction_narration ilike '%air freshener%'
              or transaction_narration ilike '%windolene%'
              or transaction_narration ilike '%cut 4%'
              or transaction_narration ilike '%hamper for%'
              or transaction_narration ilike '%eggs%'
              or transaction_narration ilike '%milk powder%'
              or transaction_narration ilike '%condensed milk%'
              or transaction_narration ilike '%penne%'
              or transaction_narration ilike '%nutmeg%'
              or transaction_narration ilike '%seasoning%'
              or transaction_narration ilike '%indomit%'
              or transaction_narration ilike '%thyme%'
              or transaction_narration ilike '%margarine%'
              or transaction_narration ilike '%oregano%'
              or transaction_narration ilike '%cajun%'
              or transaction_narration ilike '%sesame%'
              or transaction_narration ilike '%table water%'
              or transaction_narration ilike '%carla%'
              or transaction_narration ilike '%detergent%'
              or transaction_narration ilike '%sniper%'
              or transaction_narration ilike '%scale%'
              or transaction_narration ilike '%yam pounder%'
              or transaction_narration ilike '%food processor%'
              or transaction_narration ilike '%AL-HAMDULILAHI%'
              or transaction_narration ilike '%DAIRO MODUPEOLA%'
              or transaction_narration ilike '%SAYED FARMS%'
              or transaction_narration ilike '%CHARLES AND CHARLOTTE%'
              or transaction_narration ilike '%ADEJARE RACHEAL%'
              or transaction_narration ilike '%CHIBUEZE%'
              or transaction_narration ilike '%OMTAKEN%'
              or transaction_narration ilike '%MSA Nylons%'
              or transaction_narration ilike '%SHE - ABEY%'
                                                               then 'Supplies & Procurement'
            when transaction_narration ilike '%fuel%'
              or transaction_narration ilike '%diesel%'
                                                               then 'Fuel & Diesel'
            when transaction_narration ilike '%delivery%'
              or transaction_narration ilike '%rider%'
              or transaction_narration ilike '%logistics%'
              or transaction_narration ilike '%waybill%'
                                                               then 'Logistics & Delivery'
            when transaction_narration ilike '%data%'
              or transaction_narration ilike '%airtime%'
                                                               then 'Data & Calls'
            when transaction_narration ilike '%repair%'
              or transaction_narration ilike '%maintenance%'
              or transaction_narration ilike '%cold room%'
              or transaction_narration ilike '%servicing%'
                                                               then 'Repairs & Maintenance'
            when transaction_narration ilike '%cleaning%'
              or transaction_narration ilike '%fumigat%'
                                                               then 'Cleaning & Hygiene'
            when transaction_narration ilike '%staff meal%'
              or transaction_narration ilike '%lunch meal%'
              or transaction_narration ilike '%bonus%'
              or transaction_narration ilike '%loan%'
              or transaction_narration ilike '%refund%'
                                                               then 'People & Payroll'
            when transaction_narration ilike '%electricity%'
              or transaction_narration ilike '%energy%'
              or transaction_narration ilike '%cutout%'
              or transaction_narration ilike '%change over%'
              or transaction_narration ilike '%flow switch%'
                                                               then 'Utilities'
            when transaction_narration ilike '%cellotape%'
              or transaction_narration ilike '%A4 paper%'
              or transaction_narration ilike '%marker%'
              or transaction_narration ilike '%staple%'
              or transaction_narration ilike '%notebook%'
              or transaction_narration ilike '%biro%'
              or transaction_narration ilike '%hairnet%'
              or transaction_narration ilike '%pos paper%'
              or transaction_narration ilike '%sticky note%'
                                                               then 'Office Supplies'
            when transaction_narration ilike '%tyre%'
              or transaction_narration ilike '%battery for%'
              or transaction_narration ilike '%shock absorber%'
              or transaction_narration ilike '%bus service%'
              or transaction_narration ilike '%bike particular%'
              or transaction_narration ilike '%document for bus%'
              or transaction_narration ilike '%glass replacement%'
              or transaction_narration ilike '%mid year for%'
              or transaction_narration ilike '%rewire%'
              or transaction_narration ilike '%mechanic%'
                                                               then 'Repairs & Maintenance'
            when transaction_narration ilike '%augmentation%'
              or transaction_narration ilike '%savings%'
                                                               then 'Internal Allocation'
            else 'Uncategorized'
        end                                                    as transaction_category,
        transaction_amount,
        transaction_fee_amount,
        transaction_reference,
        transaction_account_id_fk                              as transaction_account_id,
        transaction_completed_at_date_time::date               as transaction_date,
        'GoSource'                                             as business_unit
    from {{ ref('bv_gosource_lenco_transactions') }}
    where transaction_type   = 'debit'
      and transaction_status = 'successful'
      and transaction_completed_at_date_time is not null
      -- Exclude duplicate "ACCOUNT TRANSFERS" entries
      and transaction_narration not like 'ACCOUNT TRANSFERS%'
      -- Exclude internal sub-account transfers
      and transaction_narration not like 'From GO SOURCE%'
),

all_debits as (
    select * from ipc_debits
    union all
    select * from gosource_debits
)

select
    transaction_id_pk                                               as expense_id_pk,
    transaction_date                                                as expense_date,
    date_trunc('month', transaction_date)::date                     as expense_month,
    date_trunc('year',  transaction_date)::date                     as expense_year,
    transaction_category                                            as expense_category,
    business_unit,

    case
        when transaction_category in ('Salaries', 'Staff Welfare', 'Rider Commission')
             then 'People & Payroll'
        when transaction_category in ('Supplies & Procurement', 'Cleaning & Hygiene')
             then 'Supplies & Procurement'
        when transaction_category in ('Fuel & Diesel', 'Transport', 'Logistics & Delivery')
             then 'Logistics & Fuel'
        when transaction_category in ('Repairs & Maintenance', 'Utilities')
             then 'Operations & Maintenance'
        when transaction_category in ('Marketing')
             then 'Marketing'
        when transaction_category in ('Data & Calls', 'Docs & Compliance', 'Tech & Software', 'Office Supplies')
             then 'Admin & Tech'
        when transaction_category in ('Utilities')
             then 'Operations & Maintenance'
        when transaction_category in ('Bank Charges', 'Inward Transfer')
             then 'Bank & Finance'
        when transaction_category in ('Rent')
             then 'Rent & Facilities'
        when transaction_category in ('Internal Allocation')
             then 'Internal Allocation'
        else 'Uncategorized'
    end                                                             as expense_group,

    case
        when transaction_category in ('Salaries', 'Utilities', 'Data & Calls',
                                       'Docs & Compliance', 'Repairs & Maintenance')
             then 'Fixed'
        else 'Variable'
    end                                                             as expense_type,

    transaction_narration                                           as expense_narration,
    transaction_reference                                           as expense_reference,
    transaction_account_id                                          as expense_account_id,
    transaction_amount                                              as expense_amount,
    transaction_fee_amount                                          as expense_fee_amount

from all_debits
