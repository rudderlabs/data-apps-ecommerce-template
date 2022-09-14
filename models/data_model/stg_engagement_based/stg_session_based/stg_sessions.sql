with cte_id_stitched_tracks as (

    select distinct b.{{ var('main_id') }}, 
    {{ var('col_ecommerce_tracks_timestamp') }} 
    from {{ var('tbl_ecommerce_tracks') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_tracks_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')

), cte_incremental_ts as (

    select {{ var('main_id') }}, 
    {{ var('col_ecommerce_tracks_timestamp') }}, 
    coalesce(datediff(second, lag({{ var('col_ecommerce_tracks_timestamp') }}, 1, {{ var('col_ecommerce_tracks_timestamp') }}) over(
        partition by {{ var('main_id') }} 
        order by {{ var('col_ecommerce_tracks_timestamp') }} asc
    ),{{ var('col_ecommerce_tracks_timestamp') }}),0) as incremental_ts 
    from cte_id_stitched_tracks 
    where {{timebound( var('col_ecommerce_tracks_timestamp'))}} and {{ var('main_id')}} is not null
    order by {{ var('main_id') }}, {{ var('col_ecommerce_tracks_timestamp') }} asc

), cte_new_session as (

    select {{ var('main_id') }}, 
    {{ var('col_ecommerce_tracks_timestamp') }}, 
    case when incremental_ts >= {{ var('session_cutoff_in_sec') }} then 1 else 0 end as new_session 
    from cte_incremental_ts 

), cte_session_id as (

    select {{ var('main_id') }}, 
    {{ var('col_ecommerce_tracks_timestamp') }}, 
    sum(new_session) over(
        partition by {{ var('main_id') }} 
        order by {{ var('col_ecommerce_tracks_timestamp') }} asc
        ) as session_id 
    from cte_new_session
)

select 
    {{ var('main_id') }}, 
    session_id, 
    min({{ var('col_ecommerce_tracks_timestamp') }}) as session_start_time, 
    max({{ var('col_ecommerce_tracks_timestamp') }}) as session_end_time 
from cte_session_id 
group by 1, 2 
order by 1, 2