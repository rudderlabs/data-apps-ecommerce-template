with cte_id_stitched_product_added as (
    
    {{id_stitch( var('tbl_ecommerce_product_added'), [ var('col_ecommerce_product_added_user_id') ], var('col_ecommerce_product_added_timestamp') )}}

)

select * from cte_id_stitched_product_added