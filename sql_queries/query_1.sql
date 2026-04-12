SELECT airline, advance_purchase,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price
FROM (
    SELECT MIN(price_eur) AS price, airline, advance_purchase
    FROM all_recos
    WHERE ond = 'PAR-LIS' AND trip_type = 'RT'
      AND search_country IN ('FR', 'PT')
      AND stay_duration BETWEEN 7 AND 13
      AND nb_connections <= 1
    GROUP BY search_id, airline, advance_purchase
) t
GROUP BY airline, advance_purchase
ORDER BY airline, advance_purchase;