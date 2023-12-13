{{ config(materialized='table') }}

with __dbt__cte__city_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "postgres".public._airbyte_raw_city
select
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_log_file') as _ab_cdc_log_file,
    jsonb_extract_path_text(_airbyte_data, 'city') as city,
    jsonb_extract_path_text(_airbyte_data, 'last_update') as last_update,
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_log_pos') as _ab_cdc_log_pos,
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as _ab_cdc_updated_at,
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as _ab_cdc_deleted_at,
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_cursor') as _ab_cdc_cursor,
    jsonb_extract_path_text(_airbyte_data, 'country_id') as country_id,
    jsonb_extract_path_text(_airbyte_data, 'city_id') as city_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "postgres".public._airbyte_raw_city as table_alias
-- city
where 1 = 1
),  __dbt__cte__city_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__city_ab1
select
    cast(_ab_cdc_log_file as text) as _ab_cdc_log_file,
    cast(city as text) as city,
    cast(nullif(last_update, '') as
    timestamp with time zone
) as last_update,
    cast(_ab_cdc_log_pos as
    float
) as _ab_cdc_log_pos,
    cast(_ab_cdc_updated_at as text) as _ab_cdc_updated_at,
    cast(_ab_cdc_deleted_at as text) as _ab_cdc_deleted_at,
    cast(_ab_cdc_cursor as
    bigint
) as _ab_cdc_cursor,
    cast(country_id as
    bigint
) as country_id,
    cast(city_id as
    bigint
) as city_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__city_ab1
-- city
where 1 = 1
),  __dbt__cte__city_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__city_ab2
select
    md5(cast(coalesce(cast(_ab_cdc_log_file as text), '') || '-' || coalesce(cast(city as text), '') || '-' || coalesce(cast(last_update as text), '') || '-' || coalesce(cast(_ab_cdc_log_pos as text), '') || '-' || coalesce(cast(_ab_cdc_updated_at as text), '') || '-' || coalesce(cast(_ab_cdc_deleted_at as text), '') || '-' || coalesce(cast(_ab_cdc_cursor as text), '') || '-' || coalesce(cast(country_id as text), '') || '-' || coalesce(cast(city_id as text), '') as text)) as _airbyte_city_hashid,
    tmp.*
from __dbt__cte__city_ab2 tmp
-- city
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__city_ab3
select
    _ab_cdc_log_file,
    city,
    last_update,
    _ab_cdc_log_pos,
    _ab_cdc_updated_at,
    _ab_cdc_deleted_at,
    _ab_cdc_cursor,
    country_id,
    city_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_city_hashid
from __dbt__cte__city_ab3
-- city from "postgres".public._airbyte_raw_city
where 1 = 1
 ;