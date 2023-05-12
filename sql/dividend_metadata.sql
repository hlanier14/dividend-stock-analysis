WITH yearly_dividends AS (
    SELECT 
        ticker, 
        EXTRACT(year FROM date) AS year, 
        SUM(dividend) AS annual_dividend 
    FROM `dividend_stocks.dividends`
    WHERE EXTRACT(year FROM date) < EXTRACT(year FROM CURRENT_DATE)
    GROUP BY ticker, year
), dividend_row AS (
    SELECT 
        ticker, 
        year,
        annual_dividend,
        ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY year) AS row_number
    FROM yearly_dividends
), growth_grouping AS (
    SELECT 
        base.ticker, 
        base.year,
        MAX(restart.row_number) OVER(PARTITION BY base.ticker ORDER BY base.year) grouping_id
    FROM dividend_row AS base
    LEFT JOIN dividend_row AS restart
        ON restart.ticker = base.ticker
        AND restart.row_number = base.row_number - 1
        AND restart.annual_dividend >= base.annual_dividend
), consecutive_growth AS (
    SELECT 
        ticker, 
        COUNT(*) - 1 AS consecutive_years, 
        MIN(year) AS start_year, 
        MAX(year) AS end_year
    FROM growth_grouping
    GROUP BY ticker, grouping_id
    HAVING end_year = EXTRACT(year FROM CURRENT_DATE) - 1
    ORDER BY consecutive_years
), cagr_lag AS (
    SELECT 
        ticker,
        annual_dividend,
        year,
        LAG(annual_dividend, 4) OVER (PARTITION BY ticker ORDER BY year) AS prev_annual_dividend
    FROM yearly_dividends
), five_year_cagr AS (
    SELECT 
        ticker, 
        POWER((annual_dividend / prev_annual_dividend), 1/5.0) - 1 AS cagr_last_5_years
    FROM cagr_lag
    WHERE year = EXTRACT(year FROM CURRENT_DATE) - 1
), dividend_frequency AS (
    SELECT
        ticker, 
        COUNT(*) AS dividend_frequency
    FROM `dividend_stocks.dividends`
    WHERE EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE) - 1
    GROUP BY ticker
), cv_dividend_growth AS (
    SELECT 
        ticker, 
        STDDEV(annual_dividend) / AVG(annual_dividend) AS cv_dividend_growth
    FROM yearly_dividends
    WHERE year <= EXTRACT(year FROM CURRENT_DATE) - 1 
    AND year >= EXTRACT(year FROM CURRENT_DATE) - 5
    GROUP BY ticker
)

SELECT 
    growth.ticker,
    growth.consecutive_years AS consecutiveYears,
    cagr.cagr_last_5_years AS fiveYearCAGR,
    frequency.dividend_frequency AS dividendFrequency,
    cv.cv_dividend_growth AS fiveYearCV
FROM consecutive_growth AS growth
LEFT JOIN five_year_cagr AS cagr ON growth.ticker = cagr.ticker
LEFT JOIN dividend_frequency AS frequency ON growth.ticker = frequency.ticker
LEFT JOIN cv_dividend_growth AS cv ON growth.ticker = cv.ticker;


