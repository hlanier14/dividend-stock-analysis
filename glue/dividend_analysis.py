import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# set up Spark and GlueContext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client('s3')

# load raw data
raw_data = glueContext.create_dynamic_frame.from_catalog(database="${GlueDB}", table_name="data-${pRawFolder}").toDF()
raw_tickers = glueContext.create_dynamic_frame.from_catalog(database="${GlueDB}", table_name="ticker-${pRawFolder}").toDF()

# transform ticker columns
transformed_tickers = (
    raw_tickers
    .withColumn("row_num", F.row_number().over(Window.partitionBy("symbol").orderBy(F.col("updatedAt").desc())))
    .filter(F.col("row_num") == 1)
    .select(
        F.col('symbol').alias('ticker'), 
        F.col('security').alias('name'), 
        F.col('gics sector').alias('sector'), 
        F.col('gics sub-industry').alias('industry')
    )
)

# transform data columns
transformed_data = (
    raw_data
    .dropDuplicates(['ticker', 'date'])
    .withColumn('date', F.col('date').cast('timestamp'))
    .withColumn('year', F.year('date'))
    .withColumn('month', F.month('date'))
)

# Remove S&P and Treasury data
company_data = transformed_data.filter(~F.col('ticker').isin(['^GSPC', '^TNX']))

# annual dividends
annual_dividends = (
    company_data
    .groupBy('ticker', 'year')
    .agg(
        F.sum('dividends').alias('annual_dividend'),
        F.count(F.when(F.col('dividends') > 0, F.col('dividends'))).alias('dividendFrequency')
    )
)

# consecutive dividend growth years and 5 year CAGR
annual_window = Window.partitionBy('ticker').orderBy('year')
five_year_window = Window.partitionBy('ticker').orderBy('year').rowsBetween(-4, 0)
dividend_calculations = (
    annual_dividends
    .withColumn('dividend_increase', F.col('annual_dividend') - F.lag('annual_dividend').over(annual_window))
    .withColumn('flag', F.when(F.col('dividend_increase') > 0, 1).otherwise(0))
    .withColumn('grp', F.row_number().over(annual_window) - F.row_number().over(Window.partitionBy('ticker', 'flag').orderBy('flag')))
    .withColumn('consecutiveGrowthYears', F.when(F.col('flag') == 0, 0).otherwise(F.row_number().over(Window.partitionBy('ticker', 'flag', 'grp').orderBy('year'))))
    .where(F.col('consecutiveGrowthYears') >= 5)
    .withColumn('fiveYearCAGR',F.pow(F.last('annual_dividend').over(five_year_window) / F.first('annual_dividend').over(five_year_window), 1/5) - 1)
    .where(F.col('year') == (datetime.today().year - 1))
    .select(['ticker', 'dividendFrequency', 'consecutiveGrowthYears', 'fiveYearCAGR'])
)

# 5 year monthly beta calculation
group_window = Window.partitionBy('ticker', 'year', 'month').orderBy(F.desc('date'))
five_year_data = (
    company_data
    .filter(F.col('date') >= (datetime.today() - timedelta(days=(365*5))))
    .withColumn('returns', F.log(F.col('adj close') / F.lag('adj close').over(Window.partitionBy('ticker').orderBy('date')).cast('double')))
)

market_data = (
    transformed_data
    .where(F.col('ticker') == '^GSPC')
    .filter(F.col('date') >= (datetime.today() - timedelta(days=(365*5))))
    .withColumn('sp500_returns', F.log(F.col('adj close') / F.lag('adj close').over(Window.orderBy('date')).cast('double')))
    .select('date', 'sp500_returns')
)

beta_result = (
    five_year_data
    .join(market_data, 'date')
    .groupBy('ticker')
    .agg(
        F.covar_pop('returns', 'sp500_returns').alias('covar'),
        F.var_pop('sp500_returns').alias('variance_sp500')
    )
    .withColumn('beta', F.col('covar') / F.col('variance_sp500'))
    .select('ticker', 'beta')
)

# latest price and dividend
latest_data = (
    company_data
    .orderBy('date') 
    .groupBy('ticker')
    .agg(
        F.last('adj close').alias('lastPrice'),
        F.last(F.when(F.col('dividends') > 0, F.col('dividends')), ignorenulls=True).alias('lastDividend')
    )
)

# historical data
historical_prices = (
    company_data
    .filter(F.col('date') >= (datetime.today() - timedelta(days=365)))
    .groupBy('ticker')
    .agg(
        F.collect_list(F.when(F.col('date') >= (datetime.today() - timedelta(days=30)), F.struct('date', 'adj close'))).alias('priceHistory'),
        F.collect_list(F.when((F.col('dividends') > 0) & (F.col('date') >= (datetime.today() - timedelta(days=365))), F.struct('date', 'dividends'))).alias('dividendHistory')
    )
    .withColumn('priceHistory', F.expr('sort_array(priceHistory, true)'))
    .withColumn('dividendHistory', F.expr('sort_array(dividendHistory, true)'))
)

# join all results
result = (
    dividend_calculations
    .join(beta_result, 'ticker')
    .join(latest_data, 'ticker')
    .join(transformed_tickers, 'ticker')
    .join(historical_prices, 'ticker')
    .groupBy()
    .agg(
        F.collect_list(
            F.struct(
                F.col('ticker'),
                F.col('name'),
                F.col('sector'),
                F.col('industry'),
                F.col('consecutiveGrowthYears'),
                F.col('dividendFrequency'),
                F.col('beta'),
                F.col('fiveYearCAGR'),
                F.col('lastDividend'),
                F.col('lastPrice'),
                F.col('priceHistory'),
                F.col('dividendHistory')
            )
        ).alias('companies')
    )
)

# latest market data
sp500_cagr = (
    transformed_data
    .filter((F.col('ticker') == '^GSPC') & (F.col('date') >= (datetime.today() - timedelta(days=365 * 5))))
    .orderBy('date')
    .groupBy('ticker')
    .agg(
        (F.pow(F.last('adj close') / F.first('adj close'), 1 / 5) - 1).alias('rate')
    )
)

latest_risk_free = (
    transformed_data
    .filter(F.col('ticker') == '^TNX')
    .groupBy('ticker')
    .agg(
        (F.last(F.col('adj close')) / 100).alias('rate')
    )
)

latest_market = (
    sp500_cagr
    .union(latest_risk_free)
    .groupBy()
    .agg(
        F.collect_list(
            F.struct(
                F.col('ticker'),
                F.col('rate')
            )
        ).alias('benchmarks')
    )
)

combined_df = (
    result
    .join(latest_market)
    .withColumn('lastUpdated', F.lit(F.current_timestamp()))
)

json_content = combined_df.toJSON().collect()
json_data = '[' + ','.join(json_content) + ']'

object_key = '${pAnalysisFolder}/output.json'
s3_client.put_object(Body=json_data, Bucket="${pS3BucketName}", Key=object_key)

job.commit()
