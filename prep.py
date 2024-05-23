import pandas as pd

#This file contains information on the companies in the dataset.
#Attributes: industry, ticker, sector, exchange, name.
df_historical = df = pd.read_csv('../Dataset/historical_stocks.csv')

#Daily stock prices for a selection of several thousand stock tickers from NYSE and NASDAQ.
#Attributes: ticker, open, close, adj_close, low, high, volume, date
df_prices= pd.read_csv('../Dataset/historical_stock_prices.csv')

merged_df = pd.merge(df_historical, df_prices, on='ticker')
#number of rows
print(merged_df.shape[0])

#drop of exchange column(not interesting for our scope)
merged_df.drop(['exchange', 'adj_close'], axis=1, inplace=True)

print(merged_df['low'].head(10))

merged_df['name'] = merged_df['name'].str.replace(',', ' ')
merged_df['industry'] = merged_df['industry'].str.replace(',', ' ')
merged_df['sector'] = merged_df['sector'].str.replace(',', ' ')
merged_df.to_csv('../Dataset/out.csv')

print(merged_df.info())