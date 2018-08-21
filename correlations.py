import pandas as pd
import data.wrappers.cryptocompare_wrapper as cryptocompare_wrapper
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Get BTC values
btc_daily = cryptocompare_wrapper.daily_price_historical('BTC',
            'USD', all_data=False, limit=30, aggregate=1, exchange='bitfinex')
btc_closes = pd.Series(btc_daily.close.values)
btc_closes_changes = btc_closes.pct_change()

# Calculate correlations for BTC pairs
final_btc_pairs_to_csv = pd.read_csv('data/csv/indicators_final_btc_pairs_to_csv.csv')
i = 0
list_corr = []
list_missing = []

for index, row in final_btc_pairs_to_csv.iterrows():
    try:
        i += 1
        df_daily = cryptocompare_wrapper.daily_price_historical(row['Coin'],
            row['Pair_tuple'], all_data=False, limit=30, aggregate=1, exchange=row['Exchange'])
        closes = pd.Series(df_daily.close.values)
        closes_changes = closes.pct_change()
        corr = closes.corr(closes_changes, method='pearson', min_periods=None)
        print(i, row['Coin'], corr)
        list_corr.append([row.Coin, corr])
    except:
        list_missing.append([row.Coin, 'Missing'])
        pass

df_corr_btc = pd.DataFrame(list_corr, columns=['Coin', 'BTC pair'])
#df_corr_btc.set_index('Coin', inplace=True)

df_missing = pd.DataFrame(list_missing, columns=['Pair', 'Exchange'])
print(df_missing)


# Calculate correlations for FIAT pairs
final_fiat_pairs_to_csv = pd.read_csv('data/csv/indicators_final_fiat_pairs_to_csv.csv')
i = 0
list_corr = []
list_missing = []

for index, row in final_fiat_pairs_to_csv.iterrows():
    try:
        i += 1
        df_daily = cryptocompare_wrapper.daily_price_historical(row['Coin'],
            row['Pair_tuple'], all_data=False, limit=30, aggregate=1, exchange=row['Exchange'])
        closes = pd.Series(df_daily.close.values)
        closes_changes = closes.pct_change()
        corr = closes.corr(closes_changes, method='pearson', min_periods=None)
        print(i, row['Coin'], corr)
        list_corr.append([row.Coin, corr])
    except:
        list_missing.append([row.Coin, 'Missing'])
        pass

df_corr_fiat = pd.DataFrame(list_corr, columns=['Coin', 'Fiat pair'])
#df_corr_fiat.set_index('Coin', inplace=True)

df_missing = pd.DataFrame(list_missing, columns=['Pair', 'Exchange'])
print(df_missing)

df_corr_all = pd.merge(df_corr_btc, df_corr_fiat, how='outer', on='Coin')
print(df_corr_all)

df_corr_all.to_csv('data/csv/correlations.csv')

# Upload to spreadsheet
with open('data/csv/correlations.csv', 'rb') as f:
    csv_for_import = f.read()

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)
sheet = client.open("Correlations").sheet1
client.import_csv('1Z1z5j7l7jdGfXKP7LiVuzfMAQZEo5ogSnXGl_48QwqA', csv_for_import)
