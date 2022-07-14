 {{
    config(
        materialized='incremental'
    )
}}

-- unique_key=['TIMESTAMP', 'user_id']
-- ToDo: unique key should be user_id, timestamp