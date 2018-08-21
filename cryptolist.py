import data.wrappers.cryptocompare_wrapper as cryptocompare_wrapper
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets
import os

# Load CSVs
df_coinmarketcap = pd.read_csv('data/csv/cmc_load_coins.csv')
df_ath = pd.read_csv('data/csv/load_aths.csv')
df_market_shares = pd.read_csv('data/csv/cmc_load_markets.csv')
df_events_data_to_join = pd.read_csv('data/csv/events_data_to_join.csv')
df_all_events_all_coins = pd.read_csv('data/csv/all_events_all_coins.csv')

basepath = os.path.abspath(".")

# Get BTC price
btc_price = cryptocompare_wrapper.get_price('BTC', 'USD')


# !!!!! merge Coinmarketcap + ATH + Market shares + Events
df_coinmarketcap = pd.merge(df_coinmarketcap, df_ath, how='left', on='name')
df_coinmarketcap = pd.merge(df_coinmarketcap, df_market_shares, how='left', on='name')
df_coinmarketcap = pd.merge(df_coinmarketcap, df_events_data_to_join, how='left', on='name')

df_coinmarketcap.to_csv('data/csv/df_coinmarketcap.csv')

df_coinmarketcap['Vol/Mcap'] = df_coinmarketcap['USD.volume_24h']/df_coinmarketcap['USD.market_cap']

df_coinmarketcap = df_coinmarketcap[['rank', 'symbol', 'name', 'BTC.price', 'USD.price', 'USD.market_cap', 'Vol/Mcap', 'ATH_retrace_USD',
                                     'USD.volume_24h', 'BTC.volume_24h', 'BTC.percent_change_24h',
                                     'BTC.percent_change_7d', 'Event_count', 'Events_URL', 'BTC+ETH_pairs', 'Top_crypto_markets',
                                     'Top_fiat_pairs', 'Top_fiat_markets', 'BTC_pairs', 'ETH_pairs', 'USD_pairs', 'USDT_pairs',
                                     'CK.USDT_pairs', 'EUR_pairs', 'CNY_pairs', 'JPY_pairs', 'KRW_pairs',
                                     'circulating_supply', 'total_supply', 'max_supply']]

df_coinmarketcap.columns = ['Rank', 'Symbol', 'Name', 'BTC Price', 'USD Price', 'MarketCap', 'Vol/Mcap', 'ATH', 'USD Vol24h', 'BTC Vol24h', 'Ch24h',
                            'Ch7d', 'Events', 'URL', 'Crypto pairs', 'Top CryptoMarkets', 'Fiat pairs', 'Top FiatMarkets', 'BTC', 'ETH', 'USD', 'USDT',
                            'CK.USDT', 'EUR', 'CNY', 'JPY', 'KRW', 'CircSupply', 'TotalSupply', 'MaxSupply']
df_coinmarketcap.set_index('Rank', drop=True, append=False, inplace=True)
df_coinmarketcap.loc[:, 'MarketCap']/= 1000000
#df_coinmarketcap.loc[:, 'USD Vol24h']*= btc_price
df_coinmarketcap.loc[:, 'CircSupply']/= 1000000
df_coinmarketcap.loc[:, 'TotalSupply']/= 1000000
df_coinmarketcap.loc[:, 'MaxSupply']/= 1000000
df_coinmarketcap.loc[:, 'Crypto pairs']/= 100
df_coinmarketcap.loc[:, 'Fiat pairs']/= 100
df_coinmarketcap.loc[:, 'BTC']/= 100
df_coinmarketcap.loc[:, 'ETH']/= 100
df_coinmarketcap.loc[:, 'USD']/= 100
df_coinmarketcap.loc[:, 'USDT']/= 100
df_coinmarketcap.loc[:, 'CK.USDT']/= 100
df_coinmarketcap.loc[:, 'EUR']/= 100
df_coinmarketcap.loc[:, 'CNY']/= 100
df_coinmarketcap.loc[:, 'JPY']/= 100
df_coinmarketcap.loc[:, 'KRW']/= 100


# sort by MarketCap
df_coinmarketcap.sort_values("MarketCap", inplace=True, ascending=False)

#set formating of values
df_coinmarketcap['BTC Price'] = df_coinmarketcap['BTC Price'].apply('{:,.8f}'.format)
df_coinmarketcap['USD Price'] = df_coinmarketcap['USD Price'].apply('{:,.3f}'.format)
df_coinmarketcap['MarketCap'] = df_coinmarketcap['MarketCap'].apply('{:,.2f}'.format)
df_coinmarketcap['BTC Vol24h'] = df_coinmarketcap['BTC Vol24h'].apply('{:,.2f}'.format)
df_coinmarketcap['USD Vol24h'] = df_coinmarketcap['USD Vol24h'].apply('{:,.0f}'.format)
df_coinmarketcap['Vol/Mcap'] = df_coinmarketcap['Vol/Mcap'].apply('{:.2%}'.format)
df_coinmarketcap['Ch24h'] = df_coinmarketcap['Ch24h'].apply('{:,.2f}'.format)
df_coinmarketcap['CircSupply'] = df_coinmarketcap['CircSupply'].apply('{:,.2f}'.format)
df_coinmarketcap['TotalSupply'] = df_coinmarketcap['TotalSupply'].apply('{:,.2f}'.format)
df_coinmarketcap['MaxSupply'] = df_coinmarketcap['MaxSupply'].apply('{:,.2f}'.format)
df_coinmarketcap['Crypto pairs'] = df_coinmarketcap['Crypto pairs'].apply('{:.2%}'.format)
df_coinmarketcap['Fiat pairs'] = df_coinmarketcap['Fiat pairs'].apply('{:.2%}'.format)
df_coinmarketcap['BTC'] = df_coinmarketcap['BTC'].apply('{:.2%}'.format)
df_coinmarketcap['ETH'] = df_coinmarketcap['ETH'].apply('{:.2%}'.format)
df_coinmarketcap['USD'] = df_coinmarketcap['USD'].apply('{:.2%}'.format)
df_coinmarketcap['USDT'] = df_coinmarketcap['USDT'].apply('{:.2%}'.format)
df_coinmarketcap['CK.USDT'] = df_coinmarketcap['CK.USDT'].apply('{:.2%}'.format)
df_coinmarketcap['EUR'] = df_coinmarketcap['EUR'].apply('{:.2%}'.format)
df_coinmarketcap['CNY'] = df_coinmarketcap['CNY'].apply('{:.2%}'.format)
df_coinmarketcap['JPY'] = df_coinmarketcap['JPY'].apply('{:.2%}'.format)
df_coinmarketcap['KRW'] = df_coinmarketcap['KRW'].apply('{:.2%}'.format)

# !!!!! COINMARKETCAP Export to GOOGLE SHEETS
#need to share spreadsheet with email client_secret.json

df_coinmarketcap.to_csv('data/csv/cryptolist.csv')
with open('data/csv/cryptolist.csv', 'rb') as f:
    csv_for_import = f.read()

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)
sheet = client.open("Cryptolist").sheet1
client.import_csv('1qBtCDAi5LTEkhW_90n0PdbYl156bu9fnByhDK_L4pp4', csv_for_import)

# !!!!! EVENTS Export to GOOGLE SHEETS
#need to share spreadsheet with email client_secret.json

# Export to CSV
with open('data/csv/all_events_all_coins.csv', 'rb') as f:
    csv_for_import = f.read()

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)
sheet = client.open("All coin events, ordered by MCAP").sheet1
client.import_csv('1-ZGUeBsVc_QyDmlOUAn2COH6NpUiX8unRJrEYDBJtsY', csv_for_import)

# !!!!! Formating sheet !!!!!!

# Enter sheet
gc = pygsheets.authorize(service_file='client_secret.json')
sh = gc.open('Cryptolist')
wks = sh.sheet1

# Freeze rows
wks.frozen_rows=1
wks.frozen_cols=2

# Set width
wks.adjust_column_width(0, end=None, pixel_size=36)
wks.adjust_column_width(1, end=None, pixel_size=50)
wks.adjust_column_width(3, end=None, pixel_size=78)
wks.adjust_column_width(4, end=None, pixel_size=70)
wks.adjust_column_width(5, end=None, pixel_size=78)
wks.adjust_column_width(6, end=None, pixel_size=58)
wks.adjust_column_width(7, end=None, pixel_size=58)
wks.adjust_column_width(8, end=None, pixel_size=96)
wks.adjust_column_width(9, end=None, pixel_size=80)
wks.adjust_column_width(10, end=None, pixel_size=42)
wks.adjust_column_width(11, end=None, pixel_size=42)
wks.adjust_column_width(12, end=None, pixel_size=42)
wks.adjust_column_width(13, end=None, pixel_size=42)
wks.adjust_column_width(14, end=None, pixel_size=68)
wks.adjust_column_width(15, end=None, pixel_size=100)
wks.adjust_column_width(16, end=None, pixel_size=68)
wks.adjust_column_width(17, end=None, pixel_size=100)
wks.adjust_column_width(18, end=None, pixel_size=52)
wks.adjust_column_width(19, end=None, pixel_size=52)
wks.adjust_column_width(20, end=None, pixel_size=52)
wks.adjust_column_width(21, end=None, pixel_size=52)
wks.adjust_column_width(22, end=None, pixel_size=52)
wks.adjust_column_width(23, end=None, pixel_size=52)
wks.adjust_column_width(24, end=None, pixel_size=52)
wks.adjust_column_width(25, end=None, pixel_size=52)
wks.adjust_column_width(25, end=None, pixel_size=52)
wks.adjust_column_width(26, end=None, pixel_size=52)
wks.adjust_column_width(27, end=None, pixel_size=80)
wks.adjust_column_width(28, end=None, pixel_size=80)
wks.adjust_column_width(29, end=None, pixel_size=80)

# Set formating for descriptive rows and columns
a1 = wks.cell('A1')
a1.set_text_format('bold', True)
a1.color = (1,1,0,1)
a1.set_text_format('fontSize',8)
rng_names = wks.get_values('A1', 'AD1', returnas='range')
rng_names.apply_format(a1)

a2 = wks.cell('A2')
a2.set_text_format('bold', True)
a2.color = (0,1,0,1)
rng_coins = wks.get_values('A2', 'B1700', returnas='range')
rng_coins.apply_format(a2)

# Notes
wks.cell('F1').note = 'Market capitalization in milions USD'
wks.cell('H1').note = 'Percentage retrace from USD ATH'
wks.cell('I1').note = 'Volume in recent 24 hour in USD'
wks.cell('J1').note = 'Volume in recent 24 hour in BTC'
wks.cell('K1').note = 'USD Price percentage change in recent 24 hours'
wks.cell('L1').note = 'USD Price percentage change in recent 7 days'
wks.cell('M1').note = 'Number of upcoming events'
wks.cell('O1').note = 'Traded volume percentage of BTC and ETH pairs'
wks.cell('P1').note = 'Top 10 BTC/ETH pairs by volume, form: "Exchange/Trading pair/Volume/Percentage of total coin volume"'
wks.cell('Q1').note = 'Traded volume percentage of major fiat pairs, mentioned in this table'
wks.cell('R1').note = 'Top 10 fiat pairs by volume, form: "Exchange/Trading pair/Volume/Percentage of total coin volume"'
wks.cell('S1').note = 'Traded volume percentage of BTC pairs'
wks.cell('T1').note = 'Traded volume percentage of ETH pairs'
wks.cell('U1').note = 'Traded volume percentage of USD pairs'
wks.cell('V1').note = 'Traded volume percentage of USDT pairs'
wks.cell('W1').note = 'Traded volume percentage of CK.USDT pairs'
wks.cell('X1').note = 'Traded volume percentage of EUR pairs'
wks.cell('Y1').note = 'Traded volume percentage of CNY pairs'
wks.cell('Z1').note = 'Traded volume percentage of JPY pairs'
wks.cell('AA1').note = 'Traded volume percentage of KRW pairs'
wks.cell('AB1').note = 'Circulating supply in millions, nan = not available'
wks.cell('AC1').note = 'Total supply in millions, nan = not available'
wks.cell('AD1').note = 'Maximum supply in millions, nan = not available'