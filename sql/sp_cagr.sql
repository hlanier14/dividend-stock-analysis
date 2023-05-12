WITH sp_five_year_price AS (
  SELECT *
  FROM `dividend_stocks.prices`
  WHERE date >= DATE_SUB(CURRENT_DATE, INTERVAL 5 YEAR)
  AND ticker = '^GSPC'
  ORDER BY date
  LIMIT 1
), sp_last_price AS (
  SELECT *
  FROM `dividend_stocks.prices`
  WHERE date = (SELECT MAX(date) FROM `dividend_stocks.prices` WHERE ticker = '^GSPC')
  AND ticker = '^GSPC'
)

SELECT 
  POW((SELECT price FROM sp_last_price) / (SELECT price FROM sp_five_year_price), (1/5)) - 1 AS value