select 
    {{ var('main_id')}}, 
    max(case when lower({{ var('col_ecommerce_identifies_device_type')}}) like '%pc' then 1 else 0 end) as is_active_on_website 
from {{ ref('identifies')}}
where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}