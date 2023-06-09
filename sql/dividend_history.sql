SELECT *
FROM `dividend_stocks.dividends`
WHERE ticker IN UNNEST(DIVIDEND_PAYERS)
AND date >= DATE_SUB(CURRENT_DATE, INTERVAL 5 YEAR) 
ORDER BY date;