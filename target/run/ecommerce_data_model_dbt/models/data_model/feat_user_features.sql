begin;
    

        insert into ANALYTICS_DB.dbt_ecommerce_features.feat_user_features ("CONTEXT_DESTINATION_TYPE", "CONTEXT_DESTINATION_ID", "EVENT_TEXT", "CONTEXT_LIBRARY_VERSION", "TIMESTAMP", "ID", "EVENT", "USER_ID", "ANONYMOUS_ID")
        (
            select "CONTEXT_DESTINATION_TYPE", "CONTEXT_DESTINATION_ID", "EVENT_TEXT", "CONTEXT_LIBRARY_VERSION", "TIMESTAMP", "ID", "EVENT", "USER_ID", "ANONYMOUS_ID"
            from ANALYTICS_DB.dbt_ecommerce_features.feat_user_features__dbt_tmp
        );
    commit;