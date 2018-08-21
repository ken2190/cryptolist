import data.wrappers.coinmarketcal_wrapper as coinmarketcal_wrapper
import pandas as pd
from datetime import datetime

id = '825_4h610u76l5kwc4ocogkok4sk8og44wggso84wkwss8ssgoc0sc'
secret = 'rhttolvbajkggcw4s8sosc08gwwcgs0cwow00k8koow4cwc84'
now = datetime.now()
dateStart = now.strftime('%d/%m/%Y')
print(dateStart)
dateEnd = '31/12/2021'

dictToken = coinmarketcal_wrapper.getToken(id, secret)
token = dictToken['access_token']

# Retrieving number of pages
events = coinmarketcal_wrapper.getEvents(token, page=1, max=1, dateRangeStart=dateStart, showMetadata=True)
events_count = int(events["_metadata"]["total_count"])
page_count = events_count // 150 + 1
page_remainder = events_count % 150
print('Starting to load events data, number of pages:', page_count)

page = 1
list = list()
while page <= page_count - 1:
    events = coinmarketcal_wrapper.getEvents(token, page=page, max=150, dateRangeStart=dateStart)
    for item in range(150):
        id_cal = events[item]["id"]
        title = events[item]["title"]
        coin_name = events[item]["coins"][0]["name"]
        coin_symbol = events[item]["coins"][0]["symbol"]
        date_event = events[item]["date_event"]
        created_date = events[item]["created_date"]
        description  = events[item]["description"]
        source = events[item]["source"]
        is_hot = events[item]["is_hot"]
        vote_count = events[item]["vote_count"]
        percentage = events[item]["percentage"]
        categories = events[item]["categories"][0]["name"]
        if len(events[item]["categories"]) > 1:
            for z in range(len(events[item]["categories"])):
                if z==0: continue
                categories = str(categories) + ', ' + str(events[item]["categories"][z]["name"])
        list.append([id_cal,title, coin_name, coin_symbol, date_event, created_date, description, source, is_hot, vote_count, percentage, categories])

        if len(events[item]["coins"])> 1:
            for i in range(len(events[item]["coins"])):
                if i==0: continue
                coin_name = events[item]["coins"][i]["name"]
                coin_symbol = events[item]["coins"][i]["symbol"]
                list.append([id_cal, title, coin_name, coin_symbol, date_event, created_date, description, source, is_hot,vote_count, percentage, categories])

    print('Page no:', page, 'loaded')
    page = page + 1

events = coinmarketcal_wrapper.getEvents(token, page=page_count, max=150, dateRangeStart=dateStart)
for item in range(page_remainder):
    id_cal = events[item]["id"]
    title = events[item]["title"]
    coin_name = events[item]["coins"][0]["name"]
    coin_symbol = events[item]["coins"][0]["symbol"]
    date_event = events[item]["date_event"]
    created_date = events[item]["created_date"]
    description = events[item]["description"]
    source = events[item]["source"]
    is_hot = events[item]["is_hot"]
    vote_count = events[item]["vote_count"]
    percentage = events[item]["percentage"]
    categories = events[item]["categories"][0]["name"]
    if len(events[item]["categories"]) > 1:
        for z in range(len(events[item]["categories"])):
            if z == 0: continue
            categories = str(categories) + ', ' + str(events[item]["categories"][z]["name"])
    list.append([id_cal, title, coin_name, coin_symbol, date_event, created_date, description, source, is_hot, vote_count, percentage, categories])

    if len(events[item]["coins"]) > 1:
        for i in range(len(events[item]["coins"])):
            if i == 0: continue
            coin_name = events[item]["coins"][i]["name"]
            coin_symbol = events[item]["coins"][i]["symbol"]
            list.append([id_cal, title, coin_name, coin_symbol, date_event, created_date, description, source, is_hot, vote_count, percentage, categories])

print('Page no:', page,'loaded.')
print('Total of:', events_count,'events loaded to database.')

df_all_events_all_coins = pd.DataFrame(list)
df_all_events_all_coins =  df_all_events_all_coins[[1,11,3,2,4,5,6,7,8,9,10]]
df_all_events_all_coins.columns = ['Title', 'Categories', 'Symbol', 'Name', 'Event_date', 'Created_date', 'Description', 'Source', 'Is_hot', 'Vote_count', 'Percentage']

# Get link of coin events, event count
ser_events_count = df_all_events_all_coins.groupby('Name').size()
df_events_data_to_join = ser_events_count.to_frame()
df_events_data_to_join['name'] = df_events_data_to_join.index
list_names = df_events_data_to_join['name'].tolist()
list_events_url = []
for item_coin_name in list_names:
    item_coin_name = item_coin_name.replace(' ', "-").strip()
    event_url = 'https://coinmarketcal.com/coin/' + item_coin_name
    list_events_url.append(event_url)
df_events_data_to_join['Events_URL'] = list_events_url
df_events_data_to_join.columns = ['Event_count', 'name', 'Events_URL']
df_events_data_to_join['name'] = df_events_data_to_join['name'].astype(object)

print(df_all_events_all_coins)
df_all_events_all_coins.to_csv('data/csv/all_events_all_coins.csv')
print(df_events_data_to_join)
df_events_data_to_join.to_csv('data/csv/events_data_to_join.csv')