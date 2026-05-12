-- dim_city: pass-through de slv_city (NY, JC) para consumo Gold.

select * from {{ ref('slv_city') }}
