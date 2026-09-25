-- Failure frequency and rate.
SELECT dataset, COUNT(*) AS executions,
 SUM(CASE WHEN validation_failures > 0 OR status = 'rejected_schema' THEN 1 ELSE 0 END) AS failing_executions,
 AVG(CASE WHEN validation_failures > 0 OR status = 'rejected_schema' THEN 1.0 ELSE 0.0 END) AS failure_rate
FROM executions GROUP BY dataset ORDER BY failure_rate DESC;

SELECT dataset, AVG(duration_seconds) AS average_seconds, MAX(duration_seconds) AS worst_seconds
FROM executions GROUP BY dataset ORDER BY average_seconds DESC;

SELECT run_id, dataset, processed_records, inserted_records, updated_records,
 rejected_records, duplicate_records, validation_failures, schema_version, status
FROM executions ORDER BY completed_at, dataset;

SELECT run_id, dataset, completed_at, duration_seconds,
 duration_seconds - LAG(duration_seconds) OVER (PARTITION BY dataset ORDER BY completed_at) AS change_seconds
FROM executions ORDER BY dataset, completed_at;

SELECT dataset, run_id, error FROM executions WHERE status <> 'success';
