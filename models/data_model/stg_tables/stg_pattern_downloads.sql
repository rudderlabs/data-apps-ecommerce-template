with cte_id_stitched_pattern_downloads as (

    {{id_stitch( var('tbl_patterns_downloaded'), [ var('col_patterns_downloaded_user_id') ], var('col_patterns_downloaded_timestamp'))}}
    
)

select * from cte_id_stitched_pattern_downloads


