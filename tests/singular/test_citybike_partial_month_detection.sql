-- Test: detecta meses cerrados con conteo < 50% del median de ese MISMO mes en otros anios
-- (seasonal median, controla por invierno/verano). Requiere >= 2 anios de historia para el mes.

with monthly as (
    select
        date_trunc('month', t.started_at) as month_start,
        month(t.started_at)               as month_num,
        c.city                            as city,
        count(*)                          as n_trips
    from {{ ref('slv_trip') }} t
    join {{ ref('slv_city') }} c on t.city_id = c.city_id
    where date_trunc('month', t.started_at) < date_trunc('month', current_date)
    group by 1, 2, 3
),

seasonal_stats as (
    select
        city,
        month_num,
        count(*)        as n_years,
        median(n_trips) as median_trips
    from monthly
    group by city, month_num
)

select
    m.month_start,
    m.city,
    m.n_trips,
    s.median_trips,
    s.n_years,
    round(100.0 * m.n_trips / nullif(s.median_trips, 0), 1) as pct_of_seasonal_median
from monthly m
join seasonal_stats s
    on m.city      = s.city
   and m.month_num = s.month_num
where s.n_years >= 2
  and m.n_trips < s.median_trips * 0.5
order by m.month_start desc
