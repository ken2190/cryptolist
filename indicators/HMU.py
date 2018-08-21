import wrappers.cryptocompare_wrapper as cryptocompare_wrapper
import talib
import math
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets

df_cmc_load_coins = pd.read_csv('cmc_load_coins.csv')
df_cmc_load_coins = df_cmc_load_coins[['rank', 'symbol', 'name', 'USD.market_cap', 'USD.volume_24h']]
df_cmc_load_coins['vol/mcap'] = df_cmc_load_coins['USD.volume_24h']/df_cmc_load_coins['USD.market_cap']
list_symbols = df_cmc_load_coins.symbol.tolist()

list_of_coins = []
count = 0
for symbol in list_symbols[1:100]:
    try:
        count += 1
        print(count)
        print(symbol)
        #symbol = 'ETH'
        comparison_symbol = 'BTC'

        df_daily = cryptocompare_wrapper.daily_price_historical(symbol, comparison_symbol, all_data=False, limit=120, aggregate=1)

        closes = df_daily.close.values
        #print(closes.size)

        per = 90
        hma_per = round(math.sqrt(per))
        #print(hma_per)

        WMA1 = 2*talib.WMA(closes, timeperiod = 45)
        WMA1 = WMA1[-27:]
        #print(WMA1)
        WMA2 = talib.WMA(closes, timeperiod = 90)
        WMA2 = WMA2[-27:]
        #print(WMA2)
        WMA3 = WMA1 - WMA2
        #print(WMA3)
        HMA = talib.WMA(WMA3,timeperiod = hma_per)
        #print(HMA)

        #print(closes[-11:-1])
        if HMA[-1] > HMA[-2]:
            hma_mark = closes[-1] / HMA[-1] - 1
            #hma_mark = str(hma_mark)
            hma_mark = '{:.2%}'.format(hma_mark)
            list_of_coins.append([symbol, hma_mark])
        else:
            hma_mark_neg = int('-1')
            hma_mark_neg = '{:.2%}'.format(hma_mark_neg)
            list_of_coins.append([symbol, hma_mark_neg])
    except:
        pass
df_marks = pd.DataFrame(list_of_coins)
df_marks.columns = ['symbol', 'HMA mark']
df_cmc_load_coins = pd.merge(df_cmc_load_coins, df_marks, how='left', on='symbol')

df_cmc_load_coins['USD.market_cap'] = df_cmc_load_coins['USD.market_cap'].apply('{:,.2f}'.format)
df_cmc_load_coins['USD.volume_24h'] = df_cmc_load_coins['USD.volume_24h'].apply('{:,.2f}'.format)
df_cmc_load_coins['vol/mcap'] = df_cmc_load_coins['vol/mcap'].apply('{:.2%}'.format)
df_cmc_load_coins = df_cmc_load_coins[df_cmc_load_coins['USD.market_cap'] != 'nan']
df_cmc_load_coins = df_cmc_load_coins[df_cmc_load_coins['USD.volume_24h'] != 'nan']
df_cmc_load_coins.columns = ['Rank', 'Symbol', 'Name', 'MarketCap USD', 'Volume24h USD', 'Vol/Mcap', 'HMA']
df_cmc_load_coins.set_index('Rank', drop=True, append=False, inplace=True)

#df_cmc_load_coins = df_cmc_load_coins.dropna()

#df_cmc_load_coins = df_cmc_load_coins.replace('nan%', '0 Not available')

df_cmc_load_coins.to_csv('HMA.csv')
with open('HMA.csv', 'rb') as f:
    csv_for_import = f.read()

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)
sheet = client.open("HMA").sheet1
client.import_csv('1iQqxZuO_cY7Gm0tzG8hHk1b-KCVabWfX4A1DGYYL6ss', csv_for_import)
