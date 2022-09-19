select 
    distinct {{ var('main_id')}}, 
    first_value({{ var('col_ecommerce_identifies_state')}}) over(
        partition by {{ var('main_id')}} 
        order by case when {{ var('col_ecommerce_identifies_state')}} is not null and {{ var('col_ecommerce_identifies_state')}} != '' then 2 else 1 end desc,  {{ var('col_ecommerce_identifies_timestamp')}} desc
    ) as state
from {{ ref('identifies')}} 
where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('main_id')}} is not null