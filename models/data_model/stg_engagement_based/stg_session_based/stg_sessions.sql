with cte_id_stitched_tracks as 
(select distinct b.main_id as main_id, timestamp from {{ source('ecommerce', 'tracks') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id')),

cte_incremental_ts as 
(select main_id, timestamp, coalesce(datediff(second, lag(timestamp, 1, timestamp) over(partition by main_id order by timestamp asc),timestamp),0) as incremental_ts 
from cte_id_stitched_tracks 
where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
order by main_id, timestamp asc),

cte_new_session as 
(select main_id, timestamp, case when incremental_ts >= {{ var('session_cutoff_in_sec') }} then 1 else 0 end as new_session from cte_incremental_ts ),

cte_session_id as 
(select main_id, timestamp, sum(new_session) over(partition by main_id order by timestamp asc) as session_id from cte_new_session)

select main_id, session_id, min(timestamp) as session_start_time, max(timestamp) as session_end_time from cte_session_id 
group by 1, 2 order by 1, 2