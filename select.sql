SELECT 
    e.state,
    COUNT(s.client_id) AS tv_purchases
FROM 
    `lofty-stack-426117-q8.gold.user_profiles_enriched` e
JOIN 
    `lofty-stack-426117-q8.silver.sales` s
ON 
    e.client_id = s.client_id
WHERE 
    s.product_name = 'TV'
    AND DATE(s.purchase_date) BETWEEN '2023-09-01' AND '2023-09-10'
    AND TIMESTAMP_DIFF(CURRENT_DATE(), e.birth_date, YEAR) BETWEEN 20 AND 30
GROUP BY 
    e.state
ORDER BY 
    tv_purchases DESC
LIMIT 1;


state  tv_purchases
Idaho     28