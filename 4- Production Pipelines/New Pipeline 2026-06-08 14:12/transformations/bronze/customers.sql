CREATE OR REFRESH MATERIALIZED VIEW customers
COMMENT "The customers lookup table, ingested from customers-json"
AS
SELECT *
FROM read_files(
  '${datasets.path}/customers-json',
  format => 'json'
);
