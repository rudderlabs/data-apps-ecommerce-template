with cte_id_stitched_identifies as (

    select distinct b.{{ var('main_id')}},
    {{ var('col_ecommerce_identifies_device_type')}}, 
    {{ var('col_ecommerce_identifies_timestamp')}}
    from {{ var('tbl_ecommerce_identifies') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_identifies_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id') or (a.{{ var('col_ecommerce_identifies_email')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'email')

)

select 
    distinct {{ var('main_id')}},
    first_value({{ var('col_ecommerce_identifies_device_type')}}) over(
        partition by {{ var('main_id')}} 
        order by case when {{ var('col_ecommerce_identifies_device_type')}} is not null and {{ var('col_ecommerce_identifies_device_type')}} != '' then 2 else 1 end desc, {{ var('col_ecommerce_identifies_timestamp')}} desc
    ) as device_type
from cte_id_stitched_identifies 
where {{timebound( var('col_ecommerce_identifies_timestamp'))}} and {{ var('main_id')}} is not null