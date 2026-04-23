select
    CAST(COUNTRY_CODE_2 AS VARCHAR) as country_code, 
    CAST(COUNTRY_NAME AS VARCHAR) as country_name,
    CAST(REGION AS VARCHAR) as region_name,
    _LOADED_AT as loaded_at
from {{ source('reference_sources', 'country_codes') }}