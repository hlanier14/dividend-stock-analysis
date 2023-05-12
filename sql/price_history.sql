SELECT *
FROM `dividend_stocks.prices`
WHERE ticker IN UNNEST(DIVIDEND_PAYERS)
AND date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) 
ORDER BY date;