-- Test: detecta meses con conteo de viajes anormalmente bajo (posible upload parcial en bronze
-- como el caso 202604 donde S3 publica archivos -part1, -part2 incompletos).
-- Aplica solo a meses CERRADOS (anteriores al actual). Thresholds heuristicos conservadores:
--   NY: < 100k viajes/mes -> sospechoso (real ~1-3M)
--   JC: < 1k viajes/mes  -> sospechoso (real ~5-20k)

with monthly as (
    select
        date_trunc('month', started_at) as month_start,
        city,
        count(*) as n_trips
    from {{ ref('slv_trip') }}
    where date_trunc('month', started_at) < date_trunc('month', current_date)
    group by 1, 2
)

-- Devuelve los meses bajo umbral (cualquier fila aqui = test FAIL)
select *
from monthly
where (city = 'NY' and n_trips < 100000)
   or (city = 'JC' and n_trips < 1000)
order by month_start desc
