-- Macro: compara conteos bronze vs silver agrupados por una columna. Devuelve filas solo si hay diff.

{% macro bronze_silver_count_diff(
    bronze_relation,
    silver_relation,
    bronze_group_expr,
    silver_group_expr,
    bronze_filter='1=1',
    bronze_dedup_keys=none
) %}

with bronze_count as (
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
