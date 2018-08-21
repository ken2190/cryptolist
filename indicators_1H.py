import pandas as pd
import data.wrappers.cryptocompare_wrapper as cryptocompare_wrapper
import talib
import ta

final_pairs_to_csv = pd.read_csv('data/csv/indicators_final_pairs_to_csv.csv')
i = 0
list_indicators = []
list_missing = []

for index, row in final_pairs_to_csv.iterrows():
    try:
        i += 1
        df_daily = cryptocompare_wrapper.hourly_price_historical(row['Coin'],
            row['Pair_tuple'], limit=250, aggregate=1, exchange=row['Exchange'])
        closes = df_daily.close.values
        lows = df_daily.low.values
        highs = df_daily.low.values
        opens = df_daily.low.values
        volumetos = df_daily.volumeto.values
        rsi = talib.RSI(closes, timeperiod=14)
        ema12 = talib.EMA(closes, timeperiod=12)
        ema26 = talib.EMA(closes, timeperiod=26)
        ema50 = talib.EMA(closes, timeperiod=50)
        ema200 = talib.EMA(closes, timeperiod=200)
        atr = talib.ATR(highs, lows, closes, timeperiod=14)
        obv = talib.OBV(closes, volumetos)

        list_indicators.append([row.Coin + '/' + row.Pair_tuple, closes[-2], rsi[-2], ema50[-2], ema200[-2]])
        print(i)
    except:
        list_missing.append([row.Coin + '/' + row.Pair_tuple, row.Exchange])
        pass

df_indicators = pd.DataFrame(list_indicators, columns=['Pair', '1H Close', '1H RSI', '1H EMA50', '1H EMA200'])
df_indicators.set_index('Pair', inplace=True)
print(df_indicators)

df_missing = pd.DataFrame(list_missing, columns=['Pair', 'Exchange'])
print(df_missing)

df_indicators.to_csv('data/csv/indicators_1H.csv')
df_missing.to_csv('data/csv/indicators_missing_1H.csv')