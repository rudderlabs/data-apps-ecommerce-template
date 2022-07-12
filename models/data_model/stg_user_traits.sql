with cte_gender as 
(select user_id, gender from 
(select user_id, properties_gender as gender, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_gender is not null and properties_gender != '') where rn = 1),

cte_age as 
(select user_id, age from
(select user_id, 
(CASE WHEN dateadd(year, datediff (year, properties_birthday, getdate()), properties_birthday) > getdate()
      THEN datediff(year, properties_birthday, getdate()) - 1
      ELSE datediff(year, properties_birthday, getdate())
 END) as age, row_number() over(partition by user_id order by timestamp desc) as rn 
from {{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_birthday is not null and properties_birthday!='') where rn = 1),

cte_country as 
(select user_id, country from 
(select user_id, properties_country as country, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_country is not null and properties_country != '') where rn = 1),

cte_state as 
(select user_id, state from 
(select user_id, properties_state as state, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_state is not null and properties_state != '') where rn = 1),

cte_email_domain as 
(select user_id, email_domain from 
(select user_id, substring(properties_email, charindex( '@', properties_email) + 1, len(properties_email)) as email_domain, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_email is not null and properties_email != '') where rn = 1),

cte_is_member as 
(select user_id, is_member from
(select user_id, 
(CASE WHEN properties_is_member = TRUE
      THEN TRUE
      ELSE FALSE
END) as is_member, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and user_id is not null) where rn = 1),

cte_has_mobile_app as 
(select user_id, has_mobile_app from 
(select user_id, 
(CASE WHEN user_id IN(select user_id from {{ source('ecommerce', 'identifies') }} where context_device_type  = 'android' or context_device_type = 'iOS')
      THEN TRUE
      ELSE FALSE
      END) as has_mobile_app, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and context_device_type is not null and context_device_type != '' and user_id is not null) where rn = 1),

cte_is_active_on_website as 
(select user_id, is_active_on_website from 
(select user_id, 
(CASE WHEN user_id IN(select user_id from {{ source('ecommerce', 'identifies') }} where context_device_type like '%PC')
      THEN TRUE
      ELSE FALSE
      END)
as is_active_on_website, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and context_device_type is not null and context_device_type != '' and user_id is not null) where rn = 1),

cte_device_type as 
(select user_id, device_type from 
(select user_id, context_device_type as device_type, row_number() over(partition by user_id order by timestamp desc) as rn from 
{{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and device_type is not null and device_type != '') where rn = 1)

select coalesce(a.user_id, b.user_id, c.user_id, d.user_id, e.user_id, f.user_id, g.user_id, h.user_id, i.user_id) 
as user_id, gender, age, country, state, email_domain, is_member, has_mobile_app, is_active_on_website, device_type
from (((((((cte_gender a full outer join cte_age b on a.user_id = b.user_id) 
full outer join cte_country c on c.user_id = coalesce(a.user_id, b.user_id))
full outer join cte_state d on d.user_id = coalesce(a.user_id, b.user_id, c.user_id))
full outer join cte_email_domain e on e.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id))
full outer join cte_is_member f on f.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id, e.user_id))
full outer join cte_has_mobile_app g on g.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id, e.user_id, f.user_id))
full outer join cte_is_active_on_website as h on h.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id, e.user_id, f.user_id, g.user_id))
full outer join cte_device_type as i on i.user_id = coalesce(a.user_id, b.user_id, c.user_id,d.user_id, e.user_id, f.user_id, g.user_id, h.user_id)