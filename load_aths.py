import pandas as pd
import requests
import bs4

# !!!!! load ATHs !!!!!
ath_list = []
ath_res = requests.get('https://athcoinindex.com/price/page/all')
soup = bs4.BeautifulSoup(ath_res.text, 'html.parser')
for row in soup.findAll('table')[0].tbody.findAll('tr'):
    symbol = row.select('div a')[0].getText().strip()
    ath_retrace = row.findAll('td')[9].getText().strip()
    ath_list.append([symbol,ath_retrace])
df_ath = pd.DataFrame(ath_list, columns=['name','ATH_retrace_USD'])

print(df_ath)
df_ath.to_csv('data/csv/load_aths.csv')