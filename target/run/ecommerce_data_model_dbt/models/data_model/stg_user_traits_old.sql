
  create or replace  view ANALYTICS_DB.dbt_ecommerce_features.stg_user_traits_old 
  
   as (
    with cte_gender as ANALYTICS_DB.dbt_ecommerce_features.gender,

cte_age as ANALYTICS_DB.dbt_ecommerce_features.age,

cte_location as ANALYTICS_DB.dbt_ecommerce_features.country,

cte_email_domain as ANALYTICS_DB.dbt_ecommerce_features.email_domain,

cte_is_member as ANALYTICS_DB.dbt_ecommerce_features.is_member,

cte_has_mobile_app as ANALYTICS_DB.dbt_ecommerce_features.has_mobile_app,

cte_is_active_on_website as ANALYTICS_DB.dbt_ecommerce_features.is_active_on_website,

cte_device_type as ANALYTICS_DB.dbt_ecommerce_features.device_type

select coalesce(a.user_id, b.user_id, c.user_id, d.user_id, e.user_id, f.user_id, g.user_id, h.user_id) 
as user_id, gender, c.country as country, c.state as state, email_domain, is_member, has_mobile_app, is_active_on_website, device_type
from ((((((cte_gender a full outer join cte_age b on a.user_id = b.user_id) 
full outer join cte_location c on c.user_id = coalesce(a.user_id, b.user_id))
full outer join cte_email_domain d on d.user_id = coalesce(a.user_id, b.user_id, c.user_id))
full outer join cte_is_member e on e.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id))
full outer join cte_has_mobile_app f on f.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id, e.user_id))
full outer join cte_is_active_on_website as g on g.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id, e.user_id, f.user_id))
full outer join cte_device_type as h on h.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id, e.user_id, f.user_id, g.user_id)
  );
