-- Macro: compara el conteo de filas entre una relacion bronze y una silver agrupando
-- por una columna comun. Devuelve filas SOLO cuando hay diferencia (test FAIL en dbt).
-- Uso tipico: tests/*.sql que invocan este macro pasando source() y ref() correspondientes.

{% macro bronze_silver_count_diff(
    bronze_relation,
    silver_relation,
    bronze_group_expr,
    silver_group_expr,
    bronze_filter='1=1'
) %}

with bronze_count as (
    -- Cuenta filas en bronze aplicando el mismo filtro que stg para que la comparacion sea justa
    select {{ bronze_group_expr }} as grp, count(*) as n
    from {{ bronze_relation }}
    where {{ bronze_filter }}
    group by 1
),

silver_count as (
    -- Cuenta filas en silver (ya filtrado/casteado en stg + slv)
    select {{ silver_group_expr }} as grp, count(*) as n
    from {{ silver_relation }}
    group by 1
)

select
    coalesce(b.grp, s.grp)           as grp,
    coalesce(b.n, 0)                 as bronze_n,
    coalesce(s.n, 0)                 as silver_n,
    coalesce(s.n, 0) - coalesce(b.n, 0) as diff
from bronze_count b
full outer join silver_count s on b.grp = s.grp
where coalesce(b.n, 0) != coalesce(s.n, 0)

{% endmacro %}
