-- stg_cad_metadata.sql
-- Staging model: cleans and casts the raw Silver layer CSV data.
-- Source: silver.cad_metadata (from the Silver bucket CSV)

{{ config(materialized='view') }}

with source as (
    select * from parquet.`/opt/spark/data/silver_parquet`
),

cleaned as (
    select
        cast(filename as string)        as filename,
        cast(volume as double)          as volume,
        cast(surface_area as double)    as surface_area,
        cast(status as string)          as processing_status,

        -- Extract file extension
        substring_index(filename, '.', -1) as file_extension,

        -- Extract part id from filename (e.g., '00000000' from '00000000.step')
        substring_index(filename, '.', 1) as part_id

    from source
    where status = 'Success'
)

select * from cleaned
