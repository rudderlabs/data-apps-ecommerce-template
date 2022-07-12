select user_id, {{get_age_from_dob('properties_birthday')}} as age 
from {{ source('ecommerce', 'identifies') }} where timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}'
and properties_birthday is not null and properties_birthday!=''