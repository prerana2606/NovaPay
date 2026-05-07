{%- set yaml_metadata -%}
source_model: "stg_crm__customers"
derived_columns:
  RECORD_SOURCE: "!CRM_CUSTOMERS"
  LOAD_DATETIME: "_LOADED_AT"
hashed_columns:
  CUSTOMER_HK: "CUSTOMER_ID"
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
      - "FIRST_NAME"
      - "LAST_NAME"
      - "EMAIL"
      - "PHONE_NUMBER"
      - "CUSTOMER_TIER"
      - "DATE_OF_BIRTH"
      - "ADDRESS_CITY"
      - "ADDRESS_COUNTRY"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
