with cte_id_stitched_order_completed as (

    select distinct b.{{ var('main_id') }}, 
    {{ var('col_ecommerce_order_completed_properties_total') }}, 
    {{ var('col_ecommerce_order_completed_timestamp') }} 
    from {{ var('tbl_ecommerce_order_completed') }} a 
    left join {{ var('tbl_id_stitcher') }} b 
    on (a.{{ var('col_ecommerce_order_completed_user_id') }} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')
    
)

select 
    {{ var('main_id') }},
    median({{ var('col_ecommerce_order_completed_properties_total') }}) as median_transaction_value
from cte_id_stitched_order_completed
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}