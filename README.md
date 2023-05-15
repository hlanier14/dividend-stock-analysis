# Dividend Stock Analysis

### Introduction

I am a dividend growth investor, meaning I look to invest in stocks that have a long track record of paying (and increasing) dividends. When I invest, I want to maximize my return by buying stocks that are undervalued relative to the future value of the it's dividend payments. To do this, I can use the Gordon Growth Model, a variation of the Dividend Discount Model (DDM), to get a sense of the present value of a stock. That formula, along with a more detailed explanation of the DDM, can be found [here](https://corporatefinanceinstitute.com/resources/valuation/dividend-discount-model/).

Problem: I don't want to hand-calculate this value for the hundreds of dividend-paying stocks on the market. If I did, the stock's price would change by the time I finished!

Solution: Automatically calculate the valuation of stocks using DDM and serve those valuations to a dashboard on my portfolio website via REST API.


### Approach

To solve this problem, I extract price and dividend payment history of stocks in the S&P 500 using the yfinance Python package and store it in BigQuery. I then construct the API response by calculating  metadata for each stock along with its valuation. I store the result to Cloud Storage as a JSON to minimize SQL execution resources and decrease the APIs latency. 

This Flask API is containerized and deployed on a Cloud Run instance and I use Cloud Scheduler to automate database updates.

#### Tools
- yfinance
- Flask
- BigQuery
- Cloud Storage
- Cloud Scheduler
- Docker
- Cloud Run

Check out the UI that presents the API response [here](https://harrisonlanier.com/portfolio/dividend-analysis)!