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
        df_daily = cryptocompare_wrapper.daily_price_historical(row['Coin'],
            row['Pair_tuple'], all_data=False, limit=250, aggregate=1, exchange=row['Exchange'])
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

        #if Pair_tuple == 'BTC':
            #last_rsi = rsi[-2].fotmat
        #np.set_printoptions(formatter={'float_kind': float_formatster_rsi})
        #if row.Pair_tuple == 'BTC':
        #    ema50.set_printoptions(formatter={'float_kind': float_formatter_btc})
        #    ema200.set_printoptions(formatter={'float_kind': float_formatter_btc})
        #if row.Pair_tuple != 'BTC':
        #    ema50.set_printoptions(formatter={'float_kind': float_formatter_fiat})
        #    ema200.set_printoptions(formatter={'float_kind': float_formatter_fiat})

        #tenkan = ta.trend.ichimoku_a(df_daily.high, df_daily.low, n1=20, n2=60, fillna=False)
        #kijun = ta.trend.ichimoku_b(df_daily.high, df_daily.low, n2=60, n3=120, fillna=False)

        list_indicators.append([row.Coin + '/' + row.Pair_tuple, closes[-2], rsi[-2], ema50[-2], ema200[-2]])
        print(i)
        #if i == 30: break

    except:
        list_missing.append([row.Coin + '/' + row.Pair_tuple, row.Exchange])
#        try:
#            df_daily = cryptocompare_wrapper.daily_price_historical(row['Coin'],
#                row['Pair_tuple'], all_data=False, limit=14, aggregate=1)
#            closes = df_daily.close.values
#            # print(closes)
#            rsi = talib.RSI(closes, timeperiod=14)
#            print(index, row.Coin, rsi[-1])
#        except:
#            print(row.Coin, row.Exchange)
#            pass
        pass

df_indicators = pd.DataFrame(list_indicators, columns=['Pair', '1D Close', '1D RSI', '1D EMA50', '1D EMA200'])
df_indicators.set_index('Pair', inplace=True)
#df_indicators.sort_values(inplace=True)
print(df_indicators)

df_missing = pd.DataFrame(list_missing, columns=['Pair', 'Exchange'])
print(df_missing)

df_indicators.to_csv('data/csv/indicators_1D.csv')
df_missing.to_csv('data/csv/indicators_missing_1D.csv')

