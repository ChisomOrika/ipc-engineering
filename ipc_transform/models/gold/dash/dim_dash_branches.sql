{{ config(
    materialized='incremental',
    schema='gold',
    tags=['DAASH'],
    unique_key='branch_id_pk'
) }}

WITH branches_details AS (

SELECT
  branch_id_pk,
  branch_name,
  branch_address,
  branch_state,
  branch_lga,
  branch_latitude,
  branch_longitude,
  branch_is_headquarters,
  branch_created_at_date_time,
  branch_updated_at_date_time as branch_updated_at,
  branch_is_active
FROM {{ ref('bv_dash_branches') }} c

{% if is_incremental() %}
  WHERE branch_updated_at_date_time > (SELECT MAX(branch_updated_at) FROM {{ this }})
{% endif %}

)

SELECT * FROM branches_details
