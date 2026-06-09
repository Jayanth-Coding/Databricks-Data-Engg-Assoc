CREATE OR REFRESH STREAMING TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
SELECT
  o.order_id,
  o.quantity,
  o.customer_id,
  c.profile:first_name AS f_name,
  c.profile:last_name AS l_name,
  CAST(from_unixtime(o.order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS order_timestamp,
  o.books,
  c.profile:address:country AS country
FROM STREAM(orders_raw) o
LEFT JOIN customers c
  ON o.customer_id = c.customer_id;
