CREATE OR REFRESH STREAMING TABLE sales_bronze (
  CONSTRAINT store_present EXPECT (store_id IS NOT NULL)
) AS
SELECT *, _metadata.file_path AS _source_file
FROM STREAM read_files(
  'abfss://sokoni@sokoniadls.dfs.core.windows.net/bronze_layer/sales/',
  format => 'csv',
  header => true
);