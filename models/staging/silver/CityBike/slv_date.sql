-- Silver dimension: spine de fechas 2024-01-01 a 2026-12-31 con atributos calendario para joins en Gold y slicers en PBI

with

date_spine as (
    -- Genera todas las fechas del rango del proyecto (3 anios = 1096 dias, +1 por bisiesto 2024)
    select dateadd(day, seq4(), '2024-01-01'::date) as date_day
    from table(generator(rowcount => 1096))
)

select
    date_day as date_id,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    decode(month(date_day),
        1,'Enero', 2,'Febrero', 3,'Marzo', 4,'Abril',
        5,'Mayo', 6,'Junio', 7,'Julio', 8,'Agosto',
        9,'Septiembre',10,'Octubre', 11,'Noviembre', 12,'Diciembre'
    ) as month_name,
    to_char(date_day, 'YYYY-MM') as year_month,
    day(date_day) as day_of_month,
    dayofweekiso(date_day) as day_of_week,
    decode(dayofweekiso(date_day), 
        1,'Lunes', 2,'Martes', 3,'Miercoles', 4,'Jueves',
        5,'Viernes', 6,'Sabado', 7,'Domingo'
    ) as day_name,
    case when dayofweekiso(date_day) in (6,7) then true else false end as is_weekend,
    weekofyear(date_day) as week_of_year,
    dayofyear(date_day) as day_of_year,
    case
        when month(date_day) in (12,1,2) then 'Invierno'
        when month(date_day) in (3,4,5)  then 'Primavera'
        when month(date_day) in (6,7,8)  then 'Verano'
        else                                  'Otono'
    end as season
from date_spine
