{{ config(materialized='table', schema='bv', tags=['Python_Gosource']) }}

with bv_users as (
  select
  "_id" as users_id_pk,
  "firstName" as users_first_name,
  "lastName" as users_last_name,
  "email" as users_email,
  "customer" as users_customer_id_fk,
  "phoneNumber" as users_phone_number,
  "password" as users_password,
  "active" as users_active,
  "channel" as users_channel,
  "createdAt" as users_created_at_date_time,
  "updatedAt" as users_updated_at_date_time,
  "__v" as users___v,
  "dateOfBirth" as users_date_of_birth,
  "profilePhoto" as users_profile_photo,
  "unsubscribed" as users_unsubscribed,
  "record_load_date" as users_record_load_date
from {{ ref('raw_users') }}

)

select *
from bv_users
