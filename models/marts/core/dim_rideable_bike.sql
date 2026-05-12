-- dim_rideable_bike: pass-through de slv_rideable_type (classic, electric) para consumo Gold.

select * from {{ ref('slv_rideable_type') }}
