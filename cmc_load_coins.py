import pandas as pd
from coinmarketcap import Market
from pandas.io.json import json_normalize

coinmarketcap = Market()

listings = coinmarketcap.listings()
coins_count = int(listings["metadata"]["num_cryptocurrencies"])

pages = coins_count//100 + 1
start = 0
for i in range(pages):
    ticker = coinmarketcap.ticker(start=start, limit=100, convert='BTC', sort='id')
    df_ticker = pd.DataFrame(ticker['data'])
    df_ticker = df_ticker.T
    if i == 0:
        df_cmc_load_coins = df_ticker
    else:
        df_cmc_load_coins = df_cmc_load_coins.append(df_ticker)
    start += 100

# add Quotes
df_cmc_load_coins = df_cmc_load_coins.reset_index(drop=True)
df_1 = json_normalize(df_cmc_load_coins['quotes'])
df_cmc_load_coins = pd.concat([df_cmc_load_coins, df_1], axis=1)

print(df_cmc_load_coins)
df_cmc_load_coins.to_csv('data/csv/cmc_load_coins.csv')
