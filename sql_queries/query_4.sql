SELECT ond, airline,
       COUNT(*) AS nb_recos,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY ond), 1) AS market_share_pct
FROM all_recos
WHERE trip_type = 'RT'
GROUP BY ond, airline
HAVING COUNT(*) >= 5
ORDER BY ond, market_share_pct DESC;