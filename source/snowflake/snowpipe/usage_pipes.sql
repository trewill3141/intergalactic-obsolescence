with snowpiperuns as (
  select 
    pipe_name,
    start_time,
    end_time,
    current_time() as current_time,
    count(*) as num_runs
  from 
    information_schema.pipe_history
  where 
    pipe_name in ('your_snowpipe_name') -- replace with your snowpipe name
  group by
    pipe_name,
    start_time,
    end_time,
    current_time
),

creditusage as (
  select
    cr."timestamp" as credit_timestamp,
    cr.credit_used
  from
    information_schema.credits cr
)

select 
  sr.pipe_name,
  sr.start_time,
  sr.end_time,
  sr.current_time,
  sr.num_runs,
  avg(cu.credit_used) as avg_credit_used
from 
  snowpiperuns sr
join 
  creditusage cu
on
  sr.start_time = cu.credit_timestamp
group by
  sr.pipe_name,
  sr.start_time,
  sr.end_time,
  sr.current_time
order by 
  sr.start_time desc
limit 10; -- adjust the limit as needed
