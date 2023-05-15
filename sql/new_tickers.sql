SELECT ticker
FROM `dividend_stocks.companies`
WHERE ticker NOT IN (
  SELECT DISTINCT ticker 
  FROM `dividend_stocks.prices`
);
