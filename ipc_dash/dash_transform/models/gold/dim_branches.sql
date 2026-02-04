{{ config(materialized='table', schema='gold', tags=['Python_Gosource']) }}



WITH branches_details AS (

SELECT
  branches_id_pk,
  branches_name,
  branches_address,
  branches_state,
  branches_lga,
  branches_latitude,
  branches_longitude,
  branches_is_headquarters,
  branches_created_at_date_time,
  branches_updated_at_date_time,
  branches_is_active
FROM {{ ref('bv_branches') }} c 
)

SELECT * FROM branches_details
