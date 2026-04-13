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
-- Caveats:
--   1. Match key is normalized business name. Risk of false matches between
--      genuinely different brands with similar names. Mitigation: a manual
--      override table (TODO: brand_match_overrides) can be layered on later.
--   2. Two brands listed under different business names in each system will
--      appear as separate rows — false negatives are also possible. Spot-check
--      against the known overlap (Papa's Grill, Wings Bistro, Citysubs,
--      Spicy Corner, Ajebo Chops) on first build.
-- ============================================================================

with daash_brands as (

    select
        customer_id_pk                                                      as dash_customer_id,
        coalesce(
            nullif(trim(customer_business_name), ''),
            nullif(trim(concat(customer_first_name, ' ', customer_last_name)), '')
        )                                                                   as dash_brand_name,
        customer_email                                                      as dash_email,
        customer_phone_number                                               as dash_phone,
        customer_business_type                                              as dash_business_type,
        customer_active                                                     as dash_active,
        customer_created_at_date_time::date                                 as dash_created_date
    from {{ ref('bv_dash_customers') }}
    where customer_business_name is not null
       or (customer_first_name is not null and customer_last_name is not null)

),

gosource_brands as (

    select
        customer_id_pk                                                      as gosource_customer_id,
        customer_business_name                                              as gosource_brand_name,
        customer_verified                                                   as gosource_verified,
        customer_can_buy_on_credit                                          as gosource_credit_eligible,
        customer_credit_account                                             as gosource_credit_balance
    from {{ ref('bv_gosource_customers') }}
    where customer_business_name is not null

),

-- Normalize: lower, strip non-alphanumeric, collapse whitespace.
-- This is the join key. Display name is preserved separately.
daash_normalized as (

    select
        *,
        regexp_replace(lower(trim(dash_brand_name)), '[^a-z0-9]+', '', 'g') as brand_match_key
    from daash_brands

),

gosource_normalized as (

    select
        *,
        regexp_replace(lower(trim(gosource_brand_name)), '[^a-z0-9]+', '', 'g') as brand_match_key
    from gosource_brands

),

-- Full outer join so we capture brands on either platform or both.
joined as (

    select
        coalesce(d.brand_match_key, g.brand_match_key)                      as brand_match_key,
        coalesce(d.dash_brand_name, g.gosource_brand_name)                  as brand_name,
        d.dash_customer_id,
        g.gosource_customer_id,
        d.dash_email,
        d.dash_phone,
        d.dash_business_type,
        d.dash_active,
        d.dash_created_date,
        g.gosource_verified,
        g.gosource_credit_eligible,
        g.gosource_credit_balance,
        case when d.dash_customer_id     is not null then true else false end as on_dash,
        case when g.gosource_customer_id is not null then true else false end as on_gosource
    from daash_normalized d
    full outer join gosource_normalized g
        on d.brand_match_key = g.brand_match_key
       and length(d.brand_match_key) >= 3   -- guard against empty/very short match keys

)

select
    brand_match_key,
    brand_name,
    on_dash,
    on_gosource,
    case when on_dash and on_gosource then true else false end              as on_both_platforms,
    dash_customer_id,
    gosource_customer_id,
    dash_email,
    dash_phone,
    dash_business_type,
    dash_active,
    dash_created_date,
    gosource_verified,
    gosource_credit_eligible,
    gosource_credit_balance
from joined
where brand_match_key is not null
  and length(brand_match_key) >= 3
