SELECT ond,
       nb_connections,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
       COUNT(*) AS nb_searches
FROM (
    SELECT ond, nb_connections, MIN(price_eur) AS price
    FROM all_recos
    WHERE trip_type = 'RT'
    GROUP BY search_id, ond, nb_connections
) t
GROUP BY ond, nb_connections
HAVING COUNT(*) >= 10
ORDER BY ond, nb_connections;