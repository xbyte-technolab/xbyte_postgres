{{ config(materialized='table') }}
with __dbt__cte__customer_list_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "postgres".public._airbyte_raw_customer_list
select
    jsonb_extract_path_text(_airbyte_data, 'country') as country,
    jsonb_extract_path_text(_airbyte_data, 'zip code') as "zip code",
    jsonb_extract_path_text(_airbyte_data, 'notes') as notes,
    jsonb_extract_path_text(_airbyte_data, 'address') as address,
    jsonb_extract_path_text(_airbyte_data, 'city') as city,
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_log_pos') as _ab_cdc_log_pos,
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as _ab_cdc_deleted_at,
    jsonb_extract_path_text(_airbyte_data, 'SID') as sid,
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_log_file') as _ab_cdc_log_file,
    jsonb_extract_path_text(_airbyte_data, 'phone') as phone,
    jsonb_extract_path_text(_airbyte_data, 'name') as "name",
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as _ab_cdc_updated_at,
    jsonb_extract_path_text(_airbyte_data, 'ID') as "ID",
    jsonb_extract_path_text(_airbyte_data, '_ab_cdc_cursor') as _ab_cdc_cursor,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "postgres".public._airbyte_raw_customer_list as table_alias
-- customer_list
where 1 = 1
),  __dbt__cte__customer_list_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__customer_list_ab1
select
    cast(country as text) as country,
    cast("zip code" as text) as "zip code",
    cast(notes as text) as notes,
    cast(address as text) as address,
    cast(city as text) as city,
    cast(_ab_cdc_log_pos as
    float
) as _ab_cdc_log_pos,
    cast(_ab_cdc_deleted_at as text) as _ab_cdc_deleted_at,
    cast(sid as
    bigint
) as sid,
    cast(_ab_cdc_log_file as text) as _ab_cdc_log_file,
    cast(phone as text) as phone,
    cast("name" as text) as "name",
    cast(_ab_cdc_updated_at as text) as _ab_cdc_updated_at,
    cast("ID" as
    bigint
) as "ID",
    cast(_ab_cdc_cursor as
    bigint
) as _ab_cdc_cursor,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__customer_list_ab1
-- customer_list
where 1 = 1
),  __dbt__cte__customer_list_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__customer_list_ab2
select
    md5(cast(coalesce(cast(country as text), '') || '-' || coalesce(cast("zip code" as text), '') || '-' || coalesce(cast(notes as text), '') || '-' || coalesce(cast(address as text), '') || '-' || coalesce(cast(city as text), '') || '-' || coalesce(cast(_ab_cdc_log_pos as text), '') || '-' || coalesce(cast(_ab_cdc_deleted_at as text), '') || '-' || coalesce(cast(sid as text), '') || '-' || coalesce(cast(_ab_cdc_log_file as text), '') || '-' || coalesce(cast(phone as text), '') || '-' || coalesce(cast("name" as text), '') || '-' || coalesce(cast(_ab_cdc_updated_at as text), '') || '-' || coalesce(cast("ID" as text), '') || '-' || coalesce(cast(_ab_cdc_cursor as text), '') as text)) as _airbyte_customer_list_hashid,
    tmp.*
from __dbt__cte__customer_list_ab2 tmp
-- customer_list
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__customer_list_ab3
select
    country,
    "zip code",
    notes,
    address,
    city,
    _ab_cdc_log_pos,
    _ab_cdc_deleted_at,
    sid,
    _ab_cdc_log_file,
    phone,
    "name",
    _ab_cdc_updated_at,
    "ID",
    _ab_cdc_cursor,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_customer_list_hashid
from __dbt__cte__customer_list_ab3
-- customer_list from "postgres".public._airbyte_raw_customer_list
where 1 = 1
;