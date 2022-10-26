select 
    distinct {{ var('main_id')}}, 
    first_value({{ var('col_ecommerce_identifies_country')}}) over(
        partition by {{ var('main_id')}} 
        order by case when {{ var('col_ecommerce_identifies_country')}} is not null and  {{ var('col_ecommerce_identifies_country')}} != '' then 2 else 1 end desc, 
        {{ var('col_ecommerce_identifies_timestamp')}} desc {{frame_clause()}}
    ) as country
from {{ ref('stg_identifies')}} 
where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('main_id')}} is not null