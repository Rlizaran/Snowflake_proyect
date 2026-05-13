-- dim_user_type: passthrough de slv_user_type.

select
    user_type_code,
    member_casual,
    description,
    is_subscriber
from {{ ref('slv_user_type') }}
