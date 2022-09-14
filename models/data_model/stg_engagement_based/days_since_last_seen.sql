with cte_id_stitched_tracks as (

    select distinct b.{{ var('main_id') }}, 
    {{ var('col_ecommerce_tracks_timestamp') }} 
    from {{ var('tbl_ecommerce_tracks') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_tracks_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')

)

select 
    {{ var('main_id')}}, 
    datediff(day, date(max({{ var('col_ecommerce_tracks_timestamp') }})),date({{get_end_timestamp()}})) as days_since_last_seen
from cte_id_stitched_tracks
where {{timebound( var('col_ecommerce_tracks_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id')}}