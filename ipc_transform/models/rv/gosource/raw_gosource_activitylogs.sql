{{ config(materialized='view') }}

with raw_activitylogs as (
    select *
    from {{ source('gosource_main', 'activitylogs') }}
)

select *
from raw_activitylogs