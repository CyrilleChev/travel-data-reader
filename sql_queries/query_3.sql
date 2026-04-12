SELECT ond, advance_purchase,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
       COUNT(*) AS nb_searches
FROM (
    SELECT ond, advance_purchase, MIN(price_eur) AS price
    FROM all_recos
    WHERE trip_type = 'RT'
      AND nb_connections <= 1
    GROUP BY search_id, ond, advance_purchase
) t
GROUP BY ond, advance_purchase
HAVING COUNT(*) >= 10
ORDER BY ond, advance_purchase;