# Author : Danilo Zocco
# Date : 2022-05-15
#
# A Jupyter notebook version of this code is available at :
# https://github.com/CrazySwimmer/luxoft-home-assignment
#
#
# Versions :
# - Python 3.10.2
# - pyspark 3.2.1
# - findspark 2.0.1
#
#
# Assumptions :
# - Variables can be reused across exercises and there is no need to start from scratch each time
# - There is a 1-to-many relationship between customer and account
#     - Each customer has at least one account
#     - Each account belongs to an existing customer
# - For every currency present in the system there is an exchange rate column in fx_rate
#
#
# As per instruction, these variables already exist in the environment :
# - spark (SparkSession)
# - customer (dataframe)
# - account (dataframe)
# - fx_rate (dataframe)
#
#
# These are the imports needed for the code to work :
#
# from datetime import date
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
# from pyspark.sql.functions import col, expr, round, sequence, to_date, explode, when
#


tday = date.today() # This date is assumed to be the same throughout the exercises




# --- Exercise 1 ---

# Build a sequence of daily date from 2000-01-01 to today
# This will be used to fill the date gaps in fx_rate
sql_qry = f"SELECT sequence(to_date('2000-01-01'), to_date('{tday}'), interval 1 day) AS date"
date_full = spark.sql(sql_qry).withColumn('date', explode(col('date')))


# As per the instructions, I don't know what currencies are in the system.
# Therefore, I proceed as follow :
#  - Unpivot the fx_rate dataframe and fill the date gaps
#  - Forward fill fx rates
#  - Pivot dataframe back to original schema and save into fx_rate_clean

# Get the list of currencies in the system 
fx_currency = fx_rate.columns[1:]

# Build string of columns to unpivot with stack()
str_currency = [f"'{c}',{c}" for c in fx_currency]
str_stack = ','.join(str_currency)
unpivotExpr = f"stack({len(fx_currency)}, {str_stack}) as (cross_currency, rate)"

# Fill missing dates and unpivot
fx_rate_unpivot = date_full \
    .join(fx_rate, date_full.date == fx_rate.fx_date, 'leftouter') \
    .select(col('date').alias('fx_date'), expr(unpivotExpr))

# Fill NAs by forward filling
sql_qry = """
    SELECT
        fx_date,
        cross_currency,
        last(rate, true) OVER (PARTITION BY cross_currency ORDER BY fx_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rate
    FROM fx_rate_unpivot;"""

fx_rate_unpivot.createOrReplaceTempView('fx_rate_unpivot')
fx_rate_unpivot_clean = spark.sql(sql_qry)

# Pivot fx_rate_unpivot_clean back to the original schema
fx_rate_clean = fx_rate_unpivot_clean \
    .groupBy('fx_date') \
    .pivot('cross_currency', fx_currency) \
    .min('rate')

print('fx_rate_clean')
fx_rate_clean.sort('fx_date').show()
fx_rate_clean.printSchema()




# --- Exercise 2 ---

# Get a subset of fx rate for today
fx_rate_tday = fx_rate_unpivot_clean \
    .filter(fx_rate_unpivot_clean.fx_date == tday) \
    .withColumn('currency', col('cross_currency')[0:3]) \
    .select('currency','rate')

# Add USD with a rate of 1.0
rowUSD = spark.createDataFrame([['USD', 1.0]])
fx_rate_tday = fx_rate_tday.union(rowUSD)

# Convert balances to USD and aggregate by customer_id
customer_balance_USD = account \
    .join(fx_rate_tday, fx_rate_tday.currency == account.currency, 'leftouter') \
    .withColumn('balance_USD', col('balance') * col('rate')) \
    .groupBy('customer_id') \
    .sum('balance_USD') \
    .select('customer_id',
            round('sum(balance_USD)', 2).alias('tot_USD_balance'))

print('customer_balance_USD')
customer_balance_USD.show()




# --- Exercise 3 ---

# Get subset of customer with their local currency as per mapping in the instructions
df_cust = customer \
    .select('customer_id',
            when(customer.country_id == 'IT', 'EUR')
            .when(customer.country_id == 'FR', 'EUR')
            .when(customer.country_id == 'DE', 'EUR')
            .when(customer.country_id == 'CH', 'CHF')
            .when(customer.country_id == 'UK', 'GBP')
            .when(customer.country_id == 'JP', 'JPY')
            .otherwise('USD')
            .alias('local_currency'))

# Convert USD balances to local currency
# Reuse customer_balance_USD from Exercise 2
customer_balance_local = df_cust \
    .join(customer_balance_USD, customer_balance_USD.customer_id == customer.customer_id, 'leftouter') \
    .join(fx_rate_tday, fx_rate_tday.currency == df_cust.local_currency, 'leftouter') \
    .withColumn('tot_balance', customer_balance_USD.tot_USD_balance / fx_rate_tday.rate) \
    .select(df_cust.customer_id,
            df_cust.local_currency,
            round('tot_balance', 2).alias('tot_balance'))

print('customer_balance_local')
customer_balance_local.show()
