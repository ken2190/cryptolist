import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets

df_indicators_1H = pd.read_csv('data/csv/indicators_1H.csv')
df_indicators_4H = pd.read_csv('data/csv/indicators_4H.csv')
df_indicators_1D = pd.read_csv('data/csv/indicators_1D.csv')

df_indicators_final = pd.merge(df_indicators_1H, df_indicators_4H, how='inner')
df_indicators_final = pd.merge(df_indicators_final, df_indicators_1D, how='inner')
df_indicators_final.set_index('Pair', inplace=True)
df_indicators_final.sort_index(inplace=True)

close_to_ema50_1h = df_indicators_final['1H Close'] / df_indicators_final['1H EMA50']
df_indicators_final.insert(loc=12, column='1H Close/EMA50', value=close_to_ema50_1h)

close_to_ema200_1h = df_indicators_final['1H Close'] / df_indicators_final['1H EMA200']
df_indicators_final.insert(loc=13, column='1H Close/EMA200', value=close_to_ema200_1h)

close_to_ema50_4h = df_indicators_final['4H Close'] / df_indicators_final['4H EMA50']
df_indicators_final.insert(loc=14, column='4H Close/EMA50', value=close_to_ema50_4h)

close_to_ema200_4h = df_indicators_final['4H Close'] / df_indicators_final['4H EMA200']
df_indicators_final.insert(loc=15, column='4H Close/EMA200', value=close_to_ema200_4h)

close_to_ema50_1d = df_indicators_final['1D Close'] / df_indicators_final['1D EMA50']
df_indicators_final.insert(loc=16, column='1D Close/EMA50', value=close_to_ema50_1d)

close_to_ema200_1d = df_indicators_final['1D Close'] / df_indicators_final['1D EMA200']
df_indicators_final.insert(loc=17, column='1D Close/EMA200', value=close_to_ema200_1d)


print(df_indicators_final)


df_indicators_final.to_csv('data/csv/indicators_final.csv')
with open('data/csv/indicators_final.csv', 'rb') as f:
    csv_for_import = f.read()

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)
sheet = client.open("TA radar").sheet1
client.import_csv('1nY7NXaBfP1nnyjlTCZvrt8ar0XlIF5b2v4eajYbg2o8', csv_for_import)
