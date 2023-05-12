WITH all_tickers AS (
    SELECT ticker
    FROM `dividend_stocks.companies`
    UNION ALL
    SELECT ticker
    FROM `dividend_stocks.benchmarks`
), last_dates AS (
    SELECT
        a.ticker,
        MAX(p.date) AS lastPriceDate,
        MAX(d.date) AS lastDividendDate
    FROM all_tickers AS a
    LEFT JOIN `dividend_stocks.prices` AS p ON a.ticker = p.ticker
    LEFT JOIN `dividend_stocks.dividends` AS d ON a.ticker = d.ticker
    GROUP BY a.ticker
)

SELECT
    *,
    (CASE
        WHEN lastPriceDate < lastDividendDate THEN lastPriceDate
        ELSE lastDividendDate
    END) as earliestDate
FROM last_dates;