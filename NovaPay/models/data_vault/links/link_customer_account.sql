{{ config(materialized='incremental', schema='DATA_VAULT') }}

{%- set source_model = "stg_vault_accounts" -%}
{%- set src_pk = "CUSTOMER_ACCOUNT_LK" -%}
{%- set src_fk = ["CUSTOMER_HK", "ACCOUNT_HK"] -%}
{%- set src_ldts = "LOAD_DATETIME" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk,
                    src_fk=src_fk,
                    src_ldts=src_ldts,
                    src_source=src_source,
                    source_model=source_model) }}

