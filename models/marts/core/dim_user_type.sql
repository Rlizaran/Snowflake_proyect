-- dim_user_type: pass-through de slv_user_type (member, casual) para consumo Gold.

select * from {{ ref('slv_user_type') }}
