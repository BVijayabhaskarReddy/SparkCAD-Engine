-- fct_cad_parts.sql
-- Marts model: final Gold layer fact table for CAD parts.
-- Computes complexity score & categorizes parts by geometry.

{{ config(materialized='table') }}

with staged as (
    select * from {{ ref('stg_cad_metadata') }}
),

enriched as (
    select
        part_id,
        filename,
        volume,
        surface_area,

        -- Surface-to-volume ratio (higher = more complex geometry)
        case
            when volume > 0 then round(surface_area / volume, 4)
            else 0
        end as surface_to_volume_ratio,

        -- Complexity score: normalized metric based on geometry
        -- Scale: 1 (simple, e.g. cube) to 10 (complex, e.g. intricate bracket)
        case
            when volume <= 0 then 0
            when surface_area / volume < 1     then 1   -- Very simple (large solid)
            when surface_area / volume < 5     then 3   -- Simple
            when surface_area / volume < 15    then 5   -- Moderate
            when surface_area / volume < 50    then 7   -- Complex
            else 10                                      -- Very complex
        end as complexity_score,

        -- Part category based on volume range
        case
            when volume < 100          then 'Fastener'
            when volume < 10000        then 'Bracket'
            when volume < 100000       then 'Housing'
            else 'Large Assembly'
        end as part_category,

        -- Size classification
        case
            when volume < 100          then 'XS'
            when volume < 1000         then 'S'
            when volume < 10000        then 'M'
            when volume < 100000       then 'L'
            else 'XL'
        end as size_class,

        processing_status,
        current_timestamp() as loaded_at

    from staged
)

select * from enriched
