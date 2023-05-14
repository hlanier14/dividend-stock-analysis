WITH 
-- companies with stable, consistent dividends
dividend_payers AS (
  SELECT 
    ticker,
    dividendFrequency,
    fiveYearCAGR
  FROM `dividend_stocks.dividend_metadata`
  WHERE consecutiveYears >= 5 
  AND fiveYearCV <= .5
), 
-- five year beta
ticker_returns AS (
  SELECT
    ticker,
    date,
    price,
    LAG(price, 1) OVER (PARTITION BY ticker ORDER BY date) AS prev_price
  FROM `dividend_stocks.prices`
  WHERE date >= DATE_SUB(CURRENT_DATE, INTERVAL 5 YEAR)
  AND ticker IN (SELECT ticker FROM dividend_payers)
),
market_returns AS (
  SELECT
    date,
    price,
    LAG(price, 1) OVER (ORDER BY date) AS prev_price
  FROM `dividend_stocks.prices`
  WHERE ticker = '^GSPC' 
  AND date >= DATE_SUB(CURRENT_DATE, INTERVAL 5 YEAR)
),
joined AS (
  SELECT
    tr.ticker,
    tr.date,
    tr.price,
    tr.prev_price AS prev_ticker_price,
    mr.price AS market_price,
    mr.prev_price AS prev_market_price
  FROM ticker_returns tr
  JOIN market_returns mr
  ON tr.date = mr.date
),
beta AS (
  SELECT
    ticker,
    (COVAR_POP((price - prev_ticker_price) / prev_ticker_price, (market_price - prev_market_price) / prev_market_price)
      / VARIANCE((market_price - prev_market_price) / prev_market_price)) AS fiveYearBeta
  FROM joined
  GROUP BY ticker
), 
-- last price
last_price AS (
  SELECT 
    ticker, 
    price
  FROM `dividend_stocks.prices`
  WHERE (ticker, date) IN (
    SELECT (ticker, MAX(date))
    FROM `dividend_stocks.prices`
    GROUP BY ticker
  )
), 
-- last dividend
last_dividend AS (
  SELECT 
    ticker, 
    dividend
  FROM `dividend_stocks.dividends`
  WHERE (ticker, date) IN (
    SELECT (ticker, MAX(date))
    FROM `dividend_stocks.dividends`
    GROUP BY ticker
  )
), 
-- S&P 500 five year cagr as market rate
sp_five_year_price AS (
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
), 
-- last 10 year T Bill price as risk free rate
t_bill_price AS (
  SELECT price
  FROM `dividend_stocks.prices`
  WHERE date = (SELECT MAX(date) FROM `dividend_stocks.prices` WHERE ticker = '^TNX')
  AND ticker = '^TNX'
), combined AS (
  SELECT 
    p.ticker, 
    p.dividendFrequency,
    p.fiveYearCAGR,
    b.fiveYearBeta,
    l.price as lastPrice,
    d.dividend as lastDividend,
    POW((SELECT price FROM sp_last_price) / (SELECT price FROM sp_five_year_price), (1/5)) - 1 AS marketRate,
    (SELECT price / 100 FROM t_bill_price) AS riskFreeRate
  FROM dividend_payers AS p
  LEFT JOIN beta AS b ON p.ticker = b.ticker
  LEFT JOIN last_price AS l ON p.ticker = l.ticker
  LEFT JOIN last_dividend AS d ON p.ticker = d.ticker
), 
-- required rate of return to own each stock
required_rate AS (
  SELECT
    *,
    riskFreeRate + fiveYearBeta * (marketRate - riskFreeRate) AS requiredRate,
  FROM combined
),
-- dividend discount model valuation
ddm AS (
  SELECT
    *,
    (lastDividend * dividendFrequency) / (requiredRate - fiveYearCAGR) AS valuation
  FROM required_rate
),
-- valuation difference between model and last price
value_change AS (
  SELECT 
    *, 
    (valuation - lastPrice) / lastPrice AS pctChange
  FROM ddm
), 
model AS (
  SELECT
    ticker,
    lastPrice,
    lastDividend,
    fiveYearBeta,
    marketRate,
    riskFreeRate,
    requiredRate,
    valuation,
    pctChange,
    CURRENT_DATETIME() AS lastUpdated
  FROM value_change
  WHERE pctChange > 0
  ORDER BY pctChange
)

SELECT
  m.*,
  c.shortName,
  c.longName,
  c.website,
  c.industry,
  c.sector,
  c.payoutRatio,
  c.exDividendDate,
  c.forwardPE,
  c.forwardEps
FROM model AS m
LEFT JOIN `dividend_stocks.companies` AS c ON m.ticker = c.ticker;
