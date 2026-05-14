-- Test: detecta meses cerrados con conteo de viajes < 50% del median de la ciudad (carga parcial / archivo corrupto).
-- Solo evalua meses ya cerrados (mes actual excluido) y solo si la ciudad tiene >= 3 meses de historico para tener un median estable.

with monthly as (
    select
        date_trunc('month', t.started_at) as month_start,
        c.city                            as city,
        count(*)                          as n_trips
    from {{ ref('slv_trip') }} t
    join {{ ref('slv_city') }} c on t.city_id = c.city_id
    where date_trunc('month', t.started_at) < date_trunc('month', current_date)
    group by 1, 2
),

stats as (
    select
        city,
        count(*)         as n_months,
        median(n_trips)  as median_trips
    from monthly
    group by city
)

select
    m.month_start,
    m.city,
    m.n_trips,
    s.median_trips,
    round(100.0 * m.n_trips / nullif(s.median_trips, 0), 1) as pct_of_median
from monthly m
join stats   s on m.city = s.city
where s.n_months >= 3
  and m.n_trips < s.median_trips * 0.5
order by m.month_start desc
