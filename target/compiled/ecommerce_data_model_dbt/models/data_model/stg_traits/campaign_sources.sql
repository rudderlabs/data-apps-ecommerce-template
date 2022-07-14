with cte_id_stitched_identifies as 
(select distinct b.main_id as main_id, context_campaign_source from analytics_db.data_apps_simulated.identifies a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.MATERIAL_DOMAIN_PROFILE__IDSTITCHER_DC3F9DA0_12 b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id') or (a.properties_email = b.other_id and b.other_id_type = 'email'))
select main_id, array_agg(distinct context_campaign_source) as campaign_sources 
from cte_id_stitched_identifies
where main_id is not null
group by main_id