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
    first_value({{ var('col_ecommerce_order_completed_properties_total') }}) over(
        partition by {{ var('main_id') }} 
        order by case when {{ var('col_ecommerce_order_completed_properties_total') }} is not null then 2 else 1 end desc, timestamp desc
    ) as last_transaction_value
from cte_id_stitched_order_completed
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null