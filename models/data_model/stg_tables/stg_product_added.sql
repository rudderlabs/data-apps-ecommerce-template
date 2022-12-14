with cte_id_stitched_product_added as (
    
    {{id_stitch( var('tbl_ecommerce_product_added'), [ var('col_ecommerce_product_added_user_id') ], var('col_ecommerce_product_added_timestamp') )}}

), 
cte_id_stitched_product_added_old as 
(  {{id_stitch( var('tbl_ecommerce_product_added_old'), [ var('col_ecommerce_product_added_old_user_id') ], var('col_ecommerce_product_added_old_timestamp') )}})

select {{ var('main_id') }}, 
 {{ var('col_ecommerce_product_added_user_id') }},
 {{ var('col_ecommerce_product_added_properties_cart_id') }},
 {{ var('col_ecommerce_product_added_timestamp') }},
 {{ var('properties_product_ref_var') }}
   from cte_id_stitched_product_added
   union all 
select {{ var('main_id') }}, 
 {{ var('col_ecommerce_product_added_old_user_id') }},
 concat(anonymous_id,to_char("timestamp", 'YYYY-MM-DD')) as cart_id,
 {{ var('col_ecommerce_product_added_old_timestamp') }},
 {{ var('col_ecommerce_product_added_old_product') }}
   from cte_id_stitched_product_added_old
   
