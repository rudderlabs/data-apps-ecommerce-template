with cte_id_stitched_product_added as (

    select distinct b.{{ var('main_id') }}, 
    {{ var('col_ecommerce_product_added_properties_cart_id') }}, 
    {{ var('col_ecommerce_product_added_timestamp') }} 
    from {{ var('tbl_ecommerce_product_added') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_product_added_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')

)

select 
    {{ var('main_id') }},
    count(*) as total_products_added_to_cart
from cte_id_stitched_product_added
where {{timebound( var('col_ecommerce_product_added_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}