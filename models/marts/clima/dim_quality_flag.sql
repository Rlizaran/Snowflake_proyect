-- dim_quality_flag: passthrough del slv_quality_flag para Gold/BI.

select
    q_flag,
    q_flag_category,
    description
from {{ ref('slv_quality_flag') }}
