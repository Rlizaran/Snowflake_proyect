-- Silver date spine: calendario derivado de fechas presentes en NY/JC/NOAA
-- Materializado como VIEW (default del proyecto): UNION DISTINCT sobre 3 stg + atributos
-- de calendario; barato de recomputar al vuelo y siempre refleja el rango actual de los datos.

with

distinct_dates as (
    select distinct started_at::DATE as date_day
    from {{ ref('stg_CityBike__citybike_trips_ny') }}
    where started_at is not null 
      and started_at >= TO_DATE(20240101::VARCHAR, 'YYYYMMDD')
    union
    select distinct started_at::DATE as date_day
    from {{ ref('stg_CityBike__citybike_trips_jc') }}
    where started_at is not null 
      and started_at >= TO_DATE(20240101::VARCHAR, 'YYYYMMDD')
    union
    select distinct observation_date::DATE as date_day
    from {{ ref('stg_NOAA__noaa_raw_year') }}
    where observation_date is not null 
      and observation_date >= TO_DATE(20240101::VARCHAR, 'YYYYMMDD')
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
        else 'Otono'
    end as season
from  distinct_dates