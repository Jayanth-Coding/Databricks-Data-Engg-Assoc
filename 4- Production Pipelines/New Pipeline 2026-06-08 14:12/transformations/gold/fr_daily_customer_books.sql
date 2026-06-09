CREATE OR REFRESH MATERIALIZED VIEW fr_daily_customer_books
COMMENT "Daily number of books per customer in France"
AS
SELECT
  customer_id,
  f_name,
  l_name,
  date_trunc('DD', order_timestamp) AS order_date,
  SUM(quantity) AS books_counts
FROM orders_cleaned
WHERE country = 'France'
GROUP BY ALL;
