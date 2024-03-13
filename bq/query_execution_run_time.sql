insert into bq_cost_optimization.bq_alerting (incident_type, project_id, job_id, message, log_ts, incident_processed)
select  'query_execution_run_time' as incident_type,
          project_id,
          job_id,
          FORMAT('Query Execution Run Time Threshold Exceeded in project: %s, Job Id: %s, Job Start: %s, Query Execution Runtime: %i', project_id, job_id, STRING(job_start),query_execution_run_time) as message,
          current_timestamp as log_ts,
          false as incident_processed
    from (

        SELECT
                project_id, 
                job_id,
                start_time as job_start,
                timestamp_diff(current_timestamp(), start_time, second) as query_execution_run_time
            FROM `brendanlooker`.`region-EU`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
            WHERE timestamp_diff(current_timestamp(), start_time, second) > 120
            AND   end_time is null
    )
