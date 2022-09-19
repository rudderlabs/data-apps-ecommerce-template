with cte_id_stitched_tracks as (

    {{id_stitch( var('tbl_ecommerce_tracks'), [ var('col_ecommerce_tracks_user_id') ])}}
    
)

select * from cte_id_stitched_tracks