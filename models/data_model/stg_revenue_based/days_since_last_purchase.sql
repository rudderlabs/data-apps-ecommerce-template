with cte_id_stitched_order_completed as (

    select distinct b.{{ var('main_id') }}, 
    {{ var('col_ecommerce_order_completed_properties_total') }}, 
    {{ var('col_ecommerce_order_completed_timestamp') }} 
    from {{ var('tbl_ecommerce_order_completed') }} a 
    left join {{ var('tbl_id_stitcher') }} b 
    on (a.{{ var('col_ecommerce_order_completed_user_id') }} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')
    
)

select 
    distinct {{ var('main_id') }},
    datediff(day, date(max({{ var('col_ecommerce_order_completed_timestamp') }})),date({{get_end_timestamp()}})) as days_since_last_purchase
from cte_id_stitched_order_completed
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}