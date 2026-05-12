-- Test: detecta meses cerrados con conteo de viajes anormalmente bajo (cargas parciales en bronze).

with monthly as (
    select
        date_trunc('month', s.started_at) as month_start,
        c.city,
        count(s.*) as n_trips
    from {{ ref('slv_trip') }} s
    left join {{ ref('slv_city') }} c
      on s.city_id = c.city_id
    where date_trunc('month', started_at) < date_trunc('month', current_date)
    group by 1, 2
)

select *
from monthly
where (city = 'Manhattan'    and n_trips < 100000)
   or (city = 'Jersey City'  and n_trips < 1000)
order by month_start desc
