-- Macro: compara conteo de filas entre una relacion bronze y una silver agrupando por una
-- columna comun. Devuelve filas SOLO cuando hay diferencia (test FAIL en dbt).
-- FIX: anadido parametro opcional 'bronze_dedup_keys'. Cuando silver dedupea por una clave
-- natural (p.ej. snapshot SCD2 NOAA por scd_key, o stg merge por ride_id) y bronze tiene
-- duplicados (ej. DEV cargado dos veces), count(*) bronze != count(*) silver y el test falla
-- aunque no haya drop real. Pasando bronze_dedup_keys, la macro hace count(distinct ...) en
-- bronze sobre esa clave -> compara "observaciones unicas" vs silver y el test refleja
-- drops/silenciosos reales, no el ruido del bronze duplicado.

{% macro bronze_silver_count_diff(
    bronze_relation,
    silver_relation,
    bronze_group_expr,
    silver_group_expr,
    bronze_filter='1=1',
    bronze_dedup_keys=none
) %}

with bronze_count as (
    -- Cuenta filas (o claves unicas si se pasa bronze_dedup_keys) en bronze, mismo filtro que stg
    select
        {{ bronze_group_expr }} as grp,
        {% if bronze_dedup_keys %}
            count(distinct {{ bronze_dedup_keys | join(" || '|' || ") }}) as n
        {% else %}
            count(*) as n
        {% endif %}
    from {{ bronze_relation }}
    where {{ bronze_filter }}
    group by 1
),

silver_count as (
    -- Cuenta filas en silver (ya filtrado/casteado/deduplicado en stg + slv)
    select {{ silver_group_expr }} as grp, count(*) as n
    from {{ silver_relation }}
    group by 1
)

select
    coalesce(b.grp, s.grp)              as grp,
    coalesce(b.n, 0)                    as bronze_n,
    coalesce(s.n, 0)                    as silver_n,
    coalesce(s.n, 0) - coalesce(b.n, 0) as diff
from bronze_count b
full outer join silver_count s on b.grp = s.grp
where coalesce(b.n, 0) != coalesce(s.n, 0)

{% endmacro %}
