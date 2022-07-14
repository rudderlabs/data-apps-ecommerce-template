select distinct user_id, 
first_value(properties_country)
over(partition by user_id order by case when properties_country is not null and properties_country != '' then 2 else 1 end desc, timestamp desc) as country,
first_value(properties_state)
over(partition by user_id order by case when properties_state is not null and properties_state != '' then 2 else 1 end desc, timestamp desc) as state
from analytics_db.data_apps_simulated.identifies where timestamp >= '2021-05-01' and timestamp <= '2022-06-29'
and user_id is not null