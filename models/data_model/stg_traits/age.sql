select distinct {{ var('main_id')}}, 
    first_value({{get_age_from_dob( var('col_ecommerce_identifies_birthday'))}}) over(
        partition by {{ var('main_id')}} 
        order by case when {{ var('col_ecommerce_identifies_birthday')}} is not null then 2 else 1 end desc, 
        {{ var('col_ecommerce_identifies_timestamp')}} desc {{frame_clause()}}
    ) as age
from {{ ref('stg_identifies')}}
where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('main_id')}} is not null 
