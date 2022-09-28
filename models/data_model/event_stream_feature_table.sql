{{ config( materialized = 'incremental', 
unique_key = 'row_id' ) }} 
select
   {{ var('main_id')}},
   feature_name,
   {{get_end_timestamp()}} as "timestamp",
   feature_value_numeric,
   feature_value_string,
   feature_value_array,
   feature_type,
   {{concat_columns( [ var('main_id'), "to_char(timestamp)", "feature_name"])}} as row_id 
from
(
    select * from {{ref('stg_user_traits')}}
    union all
    select * from {{ref('stg_engagement_features')}}
    union all
    select * from {{ref('stg_pre_revenue_features')}}
    union all
    select * from {{ref('stg_revenue_features')}}
    union all
    select * from {{ref('stg_sku_based_features')}}
)