import pandas as pd
import requests
import bs4

list_exchanges = ['binance', 'bittrex', 'poloniex', 'kucoin', 'kraken', 'bitstamp', 'okex', 'coinbase-pro', 'bitfinex', 'hitbtc']
list_btc_pairs = []
list_fiat_pairs = []

# Load all BTC / Fiat pairs for each exchange
for exchange in list_exchanges:
    url = 'https://coinmarketcap.com/exchanges/' + exchange
    res_exchange = requests.get(url)
    soup = bs4.BeautifulSoup(res_exchange.text, 'html.parser')
    exchange = exchange.capitalize()
    print(url)
    print(exchange)
    for row in soup.findAll('table')[0].tbody.findAll('tr'):
        name = row.select('td')[1].getText().strip()
        name = name.capitalize()
        pair = row.select('td')[2].getText().strip()
        volume = row.select('td')[3].getText().strip()
        if volume.startswith('*'):
            dollar_sign = volume.find('$')
            volume = volume[dollar_sign:]
        volume = int(volume[1:].replace(',',''))
        if '/BTC' in pair:
            list_btc_pairs.append([exchange,name,pair,volume])
        if '/USD' in pair and '/USDT' not in pair:
            list_fiat_pairs.append([exchange, name, pair, volume])
        if '/USDT' in pair:
            list_fiat_pairs.append([exchange, name, pair, volume])
        if '/CKUSD' in pair:
            list_fiat_pairs.append([exchange, name, pair, volume])
        if '/EUR' in pair:
            list_fiat_pairs.append([exchange, name, pair, volume])
        if '/CNY' in pair:
            list_fiat_pairs.append([exchange, name, pair, volume])
        if '/JPY' in pair:
            list_fiat_pairs.append([exchange, name, pair, volume])
        if '/KRW' in pair:
            list_fiat_pairs.append([exchange, name, pair, volume])
df_btc_pairs = pd.DataFrame(list_btc_pairs, columns=['Exchange', 'Coin', 'Pair', 'Volume'])
df_fiat_pairs = pd.DataFrame(list_fiat_pairs, columns=['Exchange', 'Coin', 'Pair', 'Volume'])


# Filter highest volume BTC pairs for each coin
df_btc_pairs.sort_values(['Coin', 'Volume'], inplace=True, ascending=[True,False])
#df_btc_pairs['Volume'] = df_btc_pairs['Volume'].apply('{:,}'.format)
df_btc_pairs = df_btc_pairs.groupby(df_btc_pairs['Coin']).head(1)
df_btc_pairs.reset_index(inplace=True, drop=True)
print(df_btc_pairs)

# Getting data for function input of highest volume BTC pairs
list_exchanges_to_csv = df_btc_pairs['Exchange'].tolist()
list_pairs = df_btc_pairs['Pair'].tolist()
list_coins = []
list_pair_tuples = []

for pair in list_pairs:
    ticker_sign = pair.find('/')
    coin = pair[:ticker_sign]
    list_coins.append(coin)
    pair_tuple = pair[ticker_sign+1:]
    list_pair_tuples.append(pair_tuple)
df_final_btc_pairs_to_csv = pd.DataFrame({'Coin': list_coins, 'Pair_tuple': list_pair_tuples, 'Exchange': list_exchanges_to_csv })
print(df_final_btc_pairs_to_csv)

# Filter highest volume FIAT pairs for each coin
df_fiat_pairs.sort_values(['Coin', 'Volume'], inplace=True, ascending=[True,False])
#df_fiat_pairs['Volume'] = df_fiat_pairs['Volume'].apply('{:,}'.format)
df_fiat_pairs = df_fiat_pairs.groupby(df_fiat_pairs['Coin']).head(1)
df_fiat_pairs.reset_index(inplace=True, drop=True)
print(df_fiat_pairs)

# Getting data for function input of highest volume FIAT pairs
list_exchanges_to_csv = df_fiat_pairs['Exchange'].tolist()
list_pairs = df_fiat_pairs['Pair'].tolist()
list_coins = []
list_pair_tuples = []

for pair in list_pairs:
    ticker_sign = pair.find('/')
    coin = pair[:ticker_sign]
    list_coins.append(coin)
    pair_tuple = pair[ticker_sign+1:]
    list_pair_tuples.append(pair_tuple)
df_final_fiat_pairs_to_csv = pd.DataFrame({'Coin': list_coins, 'Pair_tuple': list_pair_tuples, 'Exchange': list_exchanges_to_csv })
print(df_final_fiat_pairs_to_csv)

# Filter highest volume pairs for each coin BTC+FIAT
df_pairs = pd.concat([df_btc_pairs, df_fiat_pairs])
df_pairs.sort_values(['Coin', 'Volume'], inplace=True, ascending=[True,False])
df_pairs['Volume'] = df_pairs['Volume'].apply('{:,}'.format)
df_pairs = df_pairs.groupby(df_pairs['Coin']).head(1)
df_pairs.reset_index(inplace=True, drop=True)
print(df_pairs)

# Getting data for function input of highest volume pairs BTC+FIAT
list_exchanges_to_csv = df_pairs['Exchange'].tolist()
list_pairs = df_pairs['Pair'].tolist()
list_coins = []
list_pair_tuples = []

for pair in list_pairs:
    ticker_sign = pair.find('/')
    coin = pair[:ticker_sign]
    list_coins.append(coin)
    pair_tuple = pair[ticker_sign+1:]
    list_pair_tuples.append(pair_tuple)
df_final_pairs_to_csv = pd.DataFrame({'Coin': list_coins, 'Pair_tuple': list_pair_tuples, 'Exchange': list_exchanges_to_csv })
print(df_final_pairs_to_csv)

# Export to csvs
df_btc_pairs.to_csv('data/csv/indicators_btc_pairs.csv')
df_fiat_pairs.to_csv('data/csv/indicators_fiat_pairs.csv')
df_pairs.to_csv('data/csv/indicators_pairs.csv')
df_final_btc_pairs_to_csv.to_csv('data/csv/indicators_final_btc_pairs_to_csv.csv')
df_final_fiat_pairs_to_csv.to_csv('data/csv/indicators_final_fiat_pairs_to_csv.csv')
df_final_pairs_to_csv.to_csv('data/csv/indicators_final_pairs_to_csv.csv')