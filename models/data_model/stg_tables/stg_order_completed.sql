with cte_id_stitched_order_completed as (
    
    {{id_stitch( var('tbl_ecommerce_order_completed'), [ var('col_ecommerce_order_completed_user_id') ])}}
)

select * from cte_id_stitched_order_completed