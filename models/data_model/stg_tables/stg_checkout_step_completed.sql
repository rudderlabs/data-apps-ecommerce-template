with cte_id_stitched_checkout_step_completed as (

    {{id_stitch( var('tbl_ecommerce_checkout_step_completed'), [ var('col_ecommerce_checkout_step_completed_user_id') ])}}
  
)
select * from cte_id_stitched_checkout_step_completed