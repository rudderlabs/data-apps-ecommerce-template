with cte_id_stitched_identifies as (

    select distinct b.{{ var('main_id')}}, 
    {{ var('col_ecommerce_checkout_step_completed_payment_method')}}, 
    {{ var('col_ecommerce_checkout_step_completed_timestamp')}} 
    from {{ var('tbl_ecommerce_checkout_step_completed') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_checkout_step_completed_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')

)

select 
    {{ var('main_id')}}, 
    array_agg(distinct {{ var('col_ecommerce_checkout_step_completed_payment_method')}}) as payment_modes 
from cte_id_stitched_identifies
where {{timebound( var('col_ecommerce_checkout_step_completed_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}