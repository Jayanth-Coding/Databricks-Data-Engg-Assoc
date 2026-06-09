CREATE OR REFRESH STREAMING TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-json-raw"
AS
SELECT *
FROM STREAM(read_files(
  '${datasets.path}/orders-json-raw',
  format => 'json'
));
