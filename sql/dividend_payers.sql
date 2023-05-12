WITH dividend_payers AS (
  SELECT
    p.*,
    c.longName,
    c.industry,
    c.sector,
    c.payoutRatio,
    c.forwardPE,
    c.forwardEps
  FROM `dividend_stocks.dividend_metadata` AS p
  LEFT JOIN `dividend_stocks.companies` AS c ON p.ticker = c.ticker
  WHERE consecutiveYears >= 5 
  AND fiveYearCV <= .5
), ticker_returns AS (
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
), last_price AS (
  SELECT 
    ticker, 
    price
  FROM `dividend_stocks.prices`
  WHERE (ticker, date) IN (
    SELECT (ticker, MAX(date))
    FROM `dividend_stocks.prices`
    GROUP BY ticker
  )
), last_dividend AS (
  SELECT 
    ticker, 
    dividend
  FROM `dividend_stocks.dividends`
  WHERE (ticker, date) IN (
    SELECT (ticker, MAX(date))
    FROM `dividend_stocks.dividends`
    GROUP BY ticker
  )
)

SELECT 
  p.*, 
  b.fiveYearBeta,
  l.price as lastPrice,
  d.dividend as lastDividend
FROM dividend_payers AS p
LEFT JOIN beta AS b ON p.ticker = b.ticker
LEFT JOIN last_price AS l ON p.ticker = l.ticker
LEFT JOIN last_dividend AS d ON p.ticker = d.ticker;
