{{ config(materialized='table', schema='gold', tags=['Finance', 'CrossPlatform']) }}

-- ============================================================================
-- dim_brands_cross_platform
-- ----------------------------------------------------------------------------
-- One row per unique brand across DAASH and GoSource, joined on a normalized
-- business name (no shared customer_id exists between the two source systems).
--
-- Why this exists:
--   The existing dim_customers UNIONs the two sources and shows a brand on
--   both platforms as two separate rows. This model resolves them into a
--   single brand entity so downstream analyses can ask:
--     - which brands are on both platforms?
--     - what is each brand's combined value (DAASH GMV + GoSource spend)?
--     - which DAASH brands could be cross-sold GoSource (and vice versa)?
--
-- Known data quality findings (surfaced, not hidden):
--   - dash_record_count > 1 means the brand has multiple DAASH customer
--     records (signed up multiple times). 41+ such cases observed on first
--     build. Worth a separate cleanup investigation.
--   - Match key is normalized business name. False matches and false negatives
--     are both possible:
--       false +: two genuinely different brands with similar names
--       false -: same brand under different names, e.g. "Hot Wings" vs
--                "Hot Wingz", "Urban Bites" vs "Urban Eats Cloud Kitchen Ltd",
--                "Citysubs" vs "Citysubs Magodo Branch"
--     Mitigation: a manual brand_match_overrides table can be layered later.
-- ============================================================================

with daash_raw as (

    select
        customer_id_pk,
        coalesce(
            nullif(trim(customer_business_name), ''),
            nullif(trim(concat(customer_first_name, ' ', customer_last_name)), '')
        )                                                                   as raw_name,
        customer_email,
        customer_phone_number,
        customer_business_type,
        customer_active,
        customer_created_at_date_time::date                                 as created_date
    from {{ ref('bv_dash_customers') }}

),

daash_keyed as (

    select
        regexp_replace(lower(trim(raw_name)), '[^a-z0-9]+', '', 'g')        as brand_match_key,
        *
    from daash_raw
    where raw_name is not null

),

-- Aggregate to one row per match key on the DAASH side.
-- Picks an arbitrary representative record + counts duplicates.
daash_agg as (

    select
        brand_match_key,
        min(raw_name)                                                       as dash_brand_name,
        count(*)                                                            as dash_record_count,
        string_agg(distinct customer_id_pk::text, ',' order by customer_id_pk::text)
                                                                            as dash_customer_ids,
        max(customer_email)                                                 as dash_email,
        max(customer_phone_number)                                          as dash_phone,
        max(customer_business_type)                                         as dash_business_type,
        bool_or(customer_active::boolean)                                   as dash_active_any,
        min(created_date)                                                   as dash_first_created
    from daash_keyed
    where length(brand_match_key) >= 3
    group by 1

),

gosource_raw as (

    select
        customer_id_pk,
        customer_business_name                                              as raw_name,
        customer_verified,
        customer_can_buy_on_credit,
        customer_credit_account
    from {{ ref('bv_gosource_customers') }}

),

gosource_keyed as (

    select
        regexp_replace(lower(trim(raw_name)), '[^a-z0-9]+', '', 'g')        as brand_match_key,
        *
    from gosource_raw
    where raw_name is not null

),

gosource_agg as (

    select
        brand_match_key,
        min(raw_name)                                                       as gosource_brand_name,
        count(*)                                                            as gosource_record_count,
        string_agg(distinct customer_id_pk::text, ',' order by customer_id_pk::text)
                                                                            as gosource_customer_ids,
        bool_or(customer_verified::boolean)                                 as gosource_verified_any,
        bool_or(customer_can_buy_on_credit::boolean)                        as gosource_credit_eligible_any,
        sum(customer_credit_account)                                        as gosource_credit_balance_total
    from gosource_keyed
    where length(brand_match_key) >= 3
    group by 1

),

joined as (

    select
        coalesce(d.brand_match_key, g.brand_match_key)                      as brand_match_key,
        coalesce(d.dash_brand_name, g.gosource_brand_name)                  as brand_name,
        case when d.brand_match_key is not null then true else false end    as on_dash,
        case when g.brand_match_key is not null then true else false end    as on_gosource,
        case when d.brand_match_key is not null
              and g.brand_match_key is not null then true else false end    as on_both_platforms,
        d.dash_record_count,
        d.dash_customer_ids,
        d.dash_email,
        d.dash_phone,
        d.dash_business_type,
        d.dash_active_any                                                   as dash_active,
        d.dash_first_created                                                as dash_created_date,
        g.gosource_record_count,
        g.gosource_customer_ids,
        g.gosource_verified_any                                             as gosource_verified,
        g.gosource_credit_eligible_any                                      as gosource_credit_eligible,
        g.gosource_credit_balance_total                                     as gosource_credit_balance
    from daash_agg d
    full outer join gosource_agg g
        on d.brand_match_key = g.brand_match_key

)

select * from joined
