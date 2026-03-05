{{ config(materialized='table', schema='bv', tags=['DAASH']) }}

with bv_customers as (
  select
    "_id" as customer_id_pk,
    "businessName" as customer_business_name,
    "email" as customer_email,
    "verified" as customer_verified,
    "gosourceConsent" as customer_gosource_consent,
    "createdAt" as customer_created_at_date_time,
    "updatedAt" as customer_updated_at_date_time,
    "__v" as customer___v,
    "firstName" as customer_first_name,
    "lastName" as customer_last_name,
    "password" as customer_password,
    "phoneNumber" as customer_phone_number,
    "role" as customer_role,
    "autoRenewalMethod" as customer_auto_renewal_method,
    "inviteAlert" as customer_invite_alert,
    "notificationToken" as customer_notification_token,
    "twoFactorTemp" as customer_two_factor_temp,
    "smsNotification" as customer_sms_notification,
    "inAppNotification" as customer_in_app_notification,
    "emailNotification" as customer_email_notification,
    "loginAlert" as customer_login_alert,
    "active" as customer_active,
    "autoRenewal" as customer_auto_renewal,
    "autoSettlement" as customer_auto_settlement,
    "hasUsedFreePlan" as customer_has_used_free_plan,
    "tax" as customer_tax,
    "twoFactorEnabled" as customer_two_factor_enabled,
    "logo" as customer_logo,
    "profilePhoto" as customer_profile_photo,
    "recoveryCodes" as customer_recovery_codes,
    "businessType" as customer_business_type,
    "record_load_date" as customer_record_load_date
  from {{ ref('raw_dash_customers') }}
)

select *
from bv_customers
