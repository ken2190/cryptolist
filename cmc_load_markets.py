import pandas as pd
import requests
import bs4


# get Coonmarketcap slugs
df_cmc_load_coins = pd.read_csv('data/csv/cmc_load_coins.csv')
list_slugs = list(df_cmc_load_coins['website_slug'])
list_market_share = list()

# get Coinmarketcap markets data
i = 0

for item in list_slugs:
    try:
        url = 'https://coinmarketcap.com/currencies/' + item + '/#markets'
        res_markets = requests.get(url)
        soup = bs4.BeautifulSoup(res_markets.text, 'html.parser')
        name = soup.select('h1')[0].getText()
        name = name[:name.find('\n',2)].strip()
        i += 1
        print(i, name)
        #crypto_share = fiat_share = btc_share = eth_share = usd_share = usdt_share = ckusd_share = eur_share = cny_share = jpy_share = krw_share = float()
        crypto_share = fiat_share = btc_share = eth_share = usd_share = usdt_share = ckusd_share = eur_share = cny_share = jpy_share = krw_share  = 0
        largest_fiat_markets = str()
        i_fiat_markets = 0
        largest_crypto_markets = str()
        i_crypto_markets = 0
        try:
            for row in soup.findAll('table')[0].tbody.findAll('tr'):
                exchange = row.select('td')[1].getText().strip()
                pair = row.select('td')[2].getText().strip()
                volume = row.select('td')[3].getText().strip()
                if volume.startswith('*'):
                    dollar_sign = volume.find('$')
                    volume = volume[dollar_sign:]
                perc = row.select('td')[5].getText().strip()
                perc = float(perc[:-1])
                perc_string = str(perc)
                if '/BTC' in pair:
                    if item == 'bitcoin':
                        continue
                    btc_share += perc
                    crypto_share += perc
                    if i_crypto_markets < 10:
                        largest_crypto_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_crypto_markets += 1
                if '/ETH' in pair:
                    if item == 'bitcoin':
                        continue
                    eth_share += perc
                    crypto_share += perc
                    if i_crypto_markets < 10:
                        largest_crypto_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_crypto_markets += 1
                if '/USD' in pair and '/USDT' not in pair:
                    usd_share += perc
                    fiat_share += perc
                    if i_fiat_markets < 10:
                        largest_fiat_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_fiat_markets += 1
                if '/USDT' in pair:
                    usdt_share += perc
                    fiat_share += perc
                    if i_fiat_markets < 10:
                        largest_fiat_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_fiat_markets += 1
                if '/CKUSD' in pair:
                    ckusd_share += perc
                    fiat_share += perc
                    if i_fiat_markets < 10:
                        largest_fiat_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_fiat_markets += 1
                if '/EUR' in pair:
                    eur_share += perc
                    fiat_share += perc
                    if i_fiat_markets < 10:
                        largest_fiat_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_fiat_markets += 1
                if '/CNY' in pair:
                    cny_share += perc
                    fiat_share += perc
                    if i_fiat_markets < 10:
                        largest_fiat_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_fiat_markets += 1
                if '/JPY' in pair:
                    jpy_share += perc
                    fiat_share += perc
                    if i_fiat_markets < 10:
                        largest_fiat_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_fiat_markets += 1
                if '/KRW' in pair:
                    krw_share += perc
                    fiat_share += perc
                    if i_fiat_markets < 10:
                        largest_fiat_markets += str(
                            exchange + ' ' + pair + ' Vol: ' + volume + ' - ' + perc_string + '%  ||  ')
                        i_fiat_markets += 1
        except:
            pass
        print(largest_crypto_markets)
        print(largest_fiat_markets)
        list_market_share.append(
            [name, crypto_share, largest_crypto_markets, fiat_share, largest_fiat_markets, btc_share, eth_share,
             usd_share, usdt_share, ckusd_share, eur_share, cny_share, jpy_share, krw_share])
    except:
        pass
    df_market_shares = pd.DataFrame(list_market_share,
                                    columns=['name', 'BTC+ETH_pairs', 'Top_crypto_markets', 'Top_fiat_pairs',
                                             'Top_fiat_markets', 'BTC_pairs', 'ETH_pairs', 'USD_pairs', 'USDT_pairs',
                                             'CK.USDT_pairs', 'EUR_pairs', 'CNY_pairs', 'JPY_pairs', 'KRW_pairs'])
print(df_market_shares)
df_market_shares.to_csv('data/csv/cmc_load_markets.csv')