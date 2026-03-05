{{ config(materialized='table', schema='bv', tags=['Lenco'], enabled=false) }}

with bv_pos_terminals as (
    select
        id                      as pos_terminal_id_pk,
        "terminalId"            as pos_terminal_terminal_id,
        "serialNumber"          as pos_terminal_serial_number,
        "terminalName"          as pos_terminal_name,
        status                  as pos_terminal_status,
        "businessName"          as pos_terminal_business_name,
        "assignedAt"            as pos_terminal_assigned_at_date_time,
        record_load_date        as pos_terminal_record_load_date
    from {{ ref('raw_lenco_pos_terminals') }}
)

select * from bv_pos_terminals
