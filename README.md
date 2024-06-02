# Historical Stock Prices Analysis

This project analyzes the "Daily Historical Stock Prices" dataset available on Kaggle, which includes historical data on stocks listed on the New York Stock Exchange (NYSE) and NASDAQ from 1970 to 2018.

## Dataset Overview

The dataset consists of two CSV files:
- `historical_stock_prices.csv`: Contains daily stock data, including ticker symbol, opening price, closing price, adjusted close, low, high, transaction volume, and date.
- `historical_stocks.csv`: Includes stock information such as ticker symbol, exchange, company name, sector, and industry.

## Technologies Used

The project was developed using several Big Data technologies:
- MapReduce
- Hive
- Spark Core
- Spark SQL

## Implemented Applications

### Statistics per Stock (Job 1)
A job that calculates annual statistics for each stock, including percentage change, minimum and maximum prices, and average volume.

### Industry Report (Job 2)
A job that generates an annual report for each industry, showing percentage change, the stock with the highest increase, and the stock with the highest transaction volume.

### Company Trends (Job 3)
A job that identifies groups of companies with similar annual variation trends for at least three consecutive years starting from 2000.

## Installation

To run the jobs on a local machine or cluster, follow these instructions:
1. Clone this repository.
2. Ensure all dependencies are installed, and you have Spark, Hadoop and Hive installed and configured on your device.
3. Execute the jobs using commands specified in their respective directories.

## Results

The results of the various jobs are documented in the final report, including charts and comparison tables of execution times.

## Useful Links

- [Kaggle Dataset](https://www.kaggle.com/datasets/ehallmar/daily-historical-stock-prices-1970-2018)
