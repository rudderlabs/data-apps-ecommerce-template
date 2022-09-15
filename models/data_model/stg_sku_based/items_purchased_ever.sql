with cte_id_stitched_order_completed as (

    select distinct b.{{ var('main_id')}}, 
    {{ var('col_ecommerce_order_completed_properties_products')}}, 
    {{ var('col_ecommerce_order_completed_timestamp')}} 
    from {{ var('tbl_ecommerce_order_completed')}} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_order_completed_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')

)

select {{ var('main_id')}},
array_agg(distinct t.value['{{var('product_ref_var')}}']) as {{ var('product_ref_var') }}
from cte_id_stitched_order_completed, TABLE(FLATTEN(parse_json({{ var('col_ecommerce_order_completed_properties_products')}}))) t
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}