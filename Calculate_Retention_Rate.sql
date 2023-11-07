-- Google BigQuery
-- Simple Version

with join_date as (
-- find out each user's first join date, each user should have 1 row only
    select user_id
        , user_join_date
    from `user_metric_table`
)
, log_date as (
    -- find out each user's all login dates
    -- eg// a user visits and revisits for 30 days then there are 30 rows
    -- the table size will be large if long period and millions of users
    select distinct user_id
        , login_date
    from `user_login_data`
)
, mid_table as (
    -- join two tables together and calculate the period apart
    -- between each login date and first date
    select distinct b.user_id
      , b.user_join_date
      , a.login_date
    -- their first login date should be the same as join date
    -- use date_diff() to calculate day-n information
    -- + 1 or not depends on you want the first day as 0 or 1, and I prefer 1
      , date_diff(a.login_date, b.user_join_date, day) + 1 as day_num
    from log_date as a
    inner join join_date as b on a.user_id = b.user_id
    -- we can use order by to briefly check if results meet expectation
    -- order by b.user_id, a.login_date
)
, base_table as (
    select distinct user_id
        , user_join_date
        , day_num
    from mid_table
)
, data_per_day as (
    select user_join_date -- represent cohorts
-- add or remove day_n based on your requirements
-- these calculates the retained user counts on day_n for each cohort
-- day_1 is the first day for each cohort
        , sum(case when day_num = 1 then 1 else 0 end) as day_1
        , sum(case when day_num = 2 then 1 else 0 end) as day_2
        , sum(case when day_num = 3 then 1 else 0 end) as day_3
        , sum(case when day_num = 4 then 1 else 0 end) as day_4
        , sum(case when day_num = 5 then 1 else 0 end) as day_5
        , sum(case when day_num = 6 then 1 else 0 end) as day_6
        , sum(case when day_num = 7 then 1 else 0 end) as day_7
        , sum(case when day_num = 14 then 1 else 0 end) as day_14
        , sum(case when day_num = 30 then 1 else 0 end) as day_30
        , sum(case when day_num = 90 then 1 else 0 end) as day_90
        , sum(case when day_num = 183 then 1 else 0 end) as day_183
        , sum(case when day_num = 365 then 1 else 0 end) as day_365
    from base_table
    group by user_join_date
)
, result_data as (
    SELECT user_join_date -- represent cohorts
        , CASE WHEN day_1 IS NOT NULL THEN day_1 ELSE 0 END as d1
-- add or remove dn based on your requirements
-- these calculates the retention rate on dn for each cohort
-- day_1 is the denominator for following days (retention rate formula)
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_2, day_1) ELSE 0 END as d2
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_3, day_1) ELSE 0 END as d3
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_4, day_1) ELSE 0 END as d4
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_5, day_1) ELSE 0 END as d5
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_6, day_1) ELSE 0 END as d6
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_7, day_1) ELSE 0 END as d7
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_14, day_1) ELSE 0 END as d14
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_30, day_1) ELSE 0 END as d30
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_90, day_1) ELSE 0 END as d90
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_183, day_1) ELSE 0 END as d183
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_365, day_1) ELSE 0 END as d365
    FROM data_per_day
)
-- unpivot is for viz design so we can use retained_days as filter
-- try it yourself with and without unpivot and you will understand why
SELECT user_join_date
    , d1 as new_user_count
    , retained_days
    , retention_rate
FROM result_data
unpivot
(
    retention_rate
    for retained_days in (d2, d3, d4, d5, d6, d7, d14, d30, d90, d183, d365)
) unpiv
;


-- Complex Version
with join_date as (
-- find out each user's first join date, more variables add complexity
-- each user should have 1 row only
    select user_id
        , user_join_date
-- you can add all potential categorical variables first
-- then in Section2, you are free to select desired ones only
        , case
            when campaign_id in (123, 456, 789) then 'special'
            when campaign_id in (100, 200) then 'organic'
            else 'regular'
          end as campaign_type
        , case 
            when country = 'KR' then 'korea'
            when country = 'JP' then 'japan'
            when country = 'US' then 'us'
            else 'others'
          end as country_others_vs_tier1
        , case 
            when all_iap_amount > 0 then 'payer'
            else 'non_payer'
          end as payer_flag
    from `user_metric_table`
)
, log_date as (
-- find out each user's all login dates
-- eg// a user visits and revisits for 30 days then there are 30 rows
-- the table size will be large if long period and millions of users
    select distinct user_id
        , login_date
    from `user_login_data`
)
, mid_table as (
    -- join two tables together and calculate the period apart
    -- between each login date and first date
    select distinct b.user_id
        , b.user_join_date
        , b.campaign_type
        , b.country_others_vs_tier1
        , b.payer_flag
        , a.login_date
    -- their first login date should be the same as join date
    -- use date_diff() to calculate day-n information
    -- + 1 or not depends on you want the first day as 0 or 1, and I prefer 1
        , date_diff(a.login_date, b.user_join_date, day) + 1 as day_num
    from log_date as a
    inner join join_date as b on a.user_id = b.user_id
    -- we can use order by to briefly check if results meet expectation
    -- order by b.user_id, a.login_date
)
, base_table as (
    select distinct user_id
        , user_join_date -- represent cohorts
-- say I stored 10 additional categorical variables in the middle table
-- I can only select fewer variables here depending on goals.
        , campaign_type
        , country_others_vs_tier1
        , payer_flag
        , day_num
    from mid_table
)
, data_per_day as (
    select user_join_date-- represent cohorts
        , campaign_type
        , country_others_vs_tier1
        , payer_flag
-- add or remove day_n based on your requirements
-- these calculates the retained user counts on day_n for each cohort
-- day_1 is the first day for each cohort
        , sum(case when day_num = 1 then 1 else 0 end) as day_1
        , sum(case when day_num = 2 then 1 else 0 end) as day_2
        , sum(case when day_num = 3 then 1 else 0 end) as day_3
        , sum(case when day_num = 4 then 1 else 0 end) as day_4
        , sum(case when day_num = 5 then 1 else 0 end) as day_5
        , sum(case when day_num = 6 then 1 else 0 end) as day_6
        , sum(case when day_num = 7 then 1 else 0 end) as day_7
        , sum(case when day_num = 14 then 1 else 0 end) as day_14
        , sum(case when day_num = 30 then 1 else 0 end) as day_30
        , sum(case when day_num = 90 then 1 else 0 end) as day_90
        , sum(case when day_num = 183 then 1 else 0 end) as day_183
        , sum(case when day_num = 365 then 1 else 0 end) as day_365
    from base_table 
    group by user_join_date, campaign_type, country_others_vs_tier1, payer_flag
)
, result_data as (
    SELECT user_join_date
        , campaign_type
        , country_others_vs_tier1
        , payer_flag
        , CASE WHEN day_1 IS NOT NULL THEN day_1 ELSE 0 END as d1
-- add or remove dn based on your requirements
-- these calculates the retention rate on dn for each cohort
-- day_1 is the denominator for following days (retention rate formula)
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_2, day_1) ELSE 0 END as d2
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_3, day_1) ELSE 0 END as d3
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_4, day_1) ELSE 0 END as d4
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_5, day_1) ELSE 0 END as d5
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_6, day_1) ELSE 0 END as d6
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_7, day_1) ELSE 0 END as d7
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_14, day_1) ELSE 0 END as d14
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_30, day_1) ELSE 0 END as d30
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_90, day_1) ELSE 0 END as d90
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_183, day_1) ELSE 0 END as d183
        , CASE WHEN day_1 IS NOT NULL THEN 
            SAFE_DIVIDE(day_365, day_1) ELSE 0 END as d365
    FROM data_per_day
)
-- unpivot is for viz design so we can use retained_days as filter
-- try it yourself with and without unpivot and you will understand why
SELECT user_join_date
    , campaign_type
    , country_others_vs_tier1
    , payer_flag
    , d1 as new_user_count
    , retained_days
    , retention_rate
FROM result_data
unpivot
(
    retention_rate
    for retained_days in (d2, d3, d4, d5, d6, d7, d14, d30, d90, d183, d365)
) unpiv
;
