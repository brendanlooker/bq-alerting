-- Export data from the target table to GCS

EXPORT DATA OPTIONS (
  uri = 'gs://bq-alerting/bq-alerting*.csv',
  format = 'CSV',
  overwrite = true
) AS

SELECT 1;
