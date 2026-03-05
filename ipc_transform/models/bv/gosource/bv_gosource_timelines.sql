{{ config(materialized='table',schema = 'bv',tags=['GoSource']) }}

with raw_timelines as (
    select * 
    from {{ ref('raw_gosource_timelines') }}  -- Reference the raw data in staging
),

bv_timelines as (
    SELECT _id as timelines_id_pk,
           title as timelines_title,
           description as timelines_description,
           "order" as order_id_fk,  -- Escape the 'order' keyword with double quotes
           "createdAt" as timelines_created_at_date_time,
           "updatedAt" as timelines_updated_at_date_time
    from raw_timelines r
)

SELECT *
from bv_timelines
