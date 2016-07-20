import time
import pandas as pd
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator
count = 0
START_SLICE = 0
END_SLICE = 2999
visited_list = []
venue_list = []


auth = Oauth1Authenticator(
    consumer_key='c-WHRqX-IBMfGiLiYHM6mQ',
    consumer_secret='SnOVHk4TojM5opg33ekkxXAiH-8',
    token='mt4rfglkxo7MyCmkdxDH9PYLMuPiTWJM',
    token_secret='ONBYU-LWdiO_ofKw_31o74jmai0'
)
client = Client(auth)

#read license dataframe
nyclic = pd.read_csv("nyc-license.csv")
licslice = nyclic[START_SLICE:END_SLICE]
for index,row in licslice.iterrows():
    curr_add = " ".join(row['add'].split())
    params = {
    'location': curr_add,
    'category_filter': 'restaurants,nightlife',
    'radius_filter': '20'
    }
    response = client.search(**params)
    #print("complete")
    if response.businesses:
        #print("ok")
        r = response.businesses[0]
        if r.id not in visited_list:
            #print("new")
            if r.location.address:
                place = {}
                place['id'] = r.id
                place['name'] = r.name
                place['rating'] = r.rating
                place['review_count'] = r.review_count
                place['categories'] = str(r.categories)
                place['address'] = r.location.address[0]
                place['orig_add'] = curr_add
                place['long'] = row['Longitude']
                place['lat'] = row['Latitude']
                 #add to visited list
                visited_list.append(r.id)
                venue_list.append(place)
                #print(place['id'])
                count += 1
                if count%25 == 0:
                    print(count)
df = pd.DataFrame(venue_list)
FILENAME = "businesses_" + str(START_SLICE) + "_" + str(END_SLICE) + "_" + str(count) + ".csv"
df.to_csv(FILENAME,encoding='utf-8')