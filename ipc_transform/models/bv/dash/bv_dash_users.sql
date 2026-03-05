{{ config(materialized='table', schema='bv', tags=['DAASH']) }}

with bv_users as (
  select
  "_id" as user_id_pk,
  "firstName" as user_first_name,
  "lastName" as user_last_name,
  "email" as user_email,
  "customer" as user_customer_id_fk,
  "phoneNumber" as user_phone_number,
  "password" as user_password,
  "active" as user_is_active,
  "channel" as user_channel,
  "createdAt" as user_created_at_date_time,
  "updatedAt" as user_updated_at_date_time,
  "__v" as user___v,
  "dateOfBirth" as user_date_of_birth,
  "profilePhoto" as user_profile_photo,
  "unsubscribed" as user_unsubscribed,
  "record_load_date" as user_record_load_date
from {{ ref('raw_dash_users') }}

)

select *
from bv_users
