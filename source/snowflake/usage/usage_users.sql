with warehousestats as (
    select qh.warehouse_name,
        qh.credit_used as total_credit_used,
        qh.start_time as warehouse_start_time,
        qh.end_time as warehouse_end_time,
        wdh.start_time as warehouse_creation_time,
        wdh.end_time as warehouse_termination_time,
        timestamp_diff(qh.end_time, qh.start_time, second) as run_time_seconds
    from information_schema.query_history qh
        join information_schema.warehouse_metering_history wdh on qh.warehouse_name = wdh.warehouse_name
)
select warehouse_name,
    total_credit_used,
    warehouse_start_time,
    warehouse_end_time,
    warehouse_creation_time,
    warehouse_termination_time,
    sum(run_time_seconds) as total_run_time_seconds,
    timestamp_diff(
        max(warehouse_end_time),
        min(warehouse_start_time),
        second
    ) as total_open_warehouse_time_seconds
from warehousestats
group by warehouse_name,
    total_credit_used,
    warehouse_start_time,
    warehouse_end_time,
    warehouse_creation_time,
    warehouse_termination_time
order by warehouse_name,
    warehouse_start_time;