-- CALCULATE CONSECUTIVE DAYS
with base as (
  select distinct userid, login_date
  from data_source_table
)
select userid
    , max(consecutivie_days) as max_days
from (
  select userid
    , grp
    , count(1) consecutivie_days -- counts the number of rows for each group under each userid
  from (
    select *
        , countif(gap_days > 1) over(partition by userid order by login_date) as grp -- under each userid's login gap_days, assign a new group when meet non consecutive days
    from (
      select userid
        , login_date
        , lag(login_date) over(partition by userid order by login_date) as pre_login_date -- sanity check could omit
        , date_diff(login_date, lag(login_date) over(partition by userid order by login_date), day) as gap_days -- for each userid calculate potential gap days, when gap_days = 1 then it's consecutive
      from base
    )
  )
  group by userid, grp
)
group by userid
order by max_days desc
;
