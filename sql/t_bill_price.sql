SELECT price
FROM `dividend_stocks.prices`
WHERE date = (SELECT MAX(date) FROM `dividend_stocks.prices` WHERE ticker = '^TNX')
AND ticker = '^TNX';