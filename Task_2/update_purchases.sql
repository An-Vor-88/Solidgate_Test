/*
Since 'uploaded_at' column fully relfects changes, i.e. new data or updated,
we don't really interested in 'created_at' or 'updated_at' that are indicators for changes in postgres.
The only use case I can see is if tables in 'redshift' would be reaploaded after some technical issue
or disaster recovery without actual data updates.
In this case we might want to use 'updated_at' instead of 'uploaded_at' as indicator,
but I think this should be a part of a recover procedure rather than a reqular pipeline.
If we can't rely neither on 'uploaded_at' nor 'updated_at', we can add row hash (FARM_FINGERPRINT, MD5) in 'purchases'
to check the actuall change. I'll leave it out of the scope of this task.
*/


-- If I need only to add WHERE clause:
SELECT
    o.*,
    t.*,
    v.*,
    CURRENT_DATETIME() as uploaded_at -- in case we also need to handle it, but better be added by job with run timestamp
FROM orders o
JOIN transactions t on o.id = t.order_id
JOIN verification v on t.id = v.transaction_id
WHERE (o.uploaded_at > (SELECT MAX(uploaded_at) FROM purchases)
    OR  t.uploaded_at >  (SELECT MAX(uploaded_at) FROM purchases)
    OR  v.uploaded_at >  (SELECT MAX(uploaded_at) FROM purchases))


-- I would rather write smth like:
WITH mart_latest_update AS (
  SELECT 
    MAX(uploaded_at) AS max_uploaded_at -- this could be stored and updated in a separate table to eliminate scan
  FROM purchases
),
combined_data AS (
  SELECT
    orders.*,
    transactions.*,
    verification.*,
    GREATEST(
      orders.uploaded_at,
      transactions.uploaded_at,
      verification.uploaded_at
    ) AS greatest_uploaded_at
  FROM orders
  JOIN transactions ON orders.id = transactions.order_id
  JOIN verification ON transactions.id = verification.transaction_id
)
SELECT
  * EXCEPT (greatest_uploaded_at, max_uploaded_at), -- we probably don't want it in mart
  CURRENT_DATETIME() as uploaded_at -- see comment above
FROM combined_data cd
JOIN mart_latest_update mlu
  ON cd.greatest_uploaded_at > mlu.max_uploaded_at