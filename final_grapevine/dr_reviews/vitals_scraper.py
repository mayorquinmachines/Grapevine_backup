import requests
from lxml import html
import sys 
import pandas as pd
import string
from addict import Dict
from elasticsearch import Elasticsearch
import csv
import time
import googlemaps

################geospatial api ###########
gmaps_key = 'AIzaSyDHmGmfTxxapify6Op8jio-Pv4rCVe0pbE'
gmaps = googlemaps.Client(key=gmaps_key)
##########################################


es = Elasticsearch()

index = sys.argv[1]
#index = 'rep'

'''body_d = Dict()
body_d.review.properties.name.type = "string"
body_d.review.properties.name.index = "not_analyzed"
body_d.review.properties.text.type = "string"
body_d.review.properties.text.index = "analyzed"
body_d.review.properties.date.type = "date"
body_d.review.properties.stars.type = "float" '''

'''body_d = {
        "review": {
            "properties": {
                "name": {"type": "string", "index":"not_analyzed"},
                "entity": {"type":"string", "index":"not_analyzed"},
                "text": {"type": "string", "index": "analyzed"},
                "date": {"type": "date"},
                "link": {"type":"string", "index":"not_analyzed"},
                "stars": {"type": "integer"},
                "location": {
                    "type": "geo_point",
                    "geohash_prefix": True,
                    "geohash_precision":  "1km"}
                }
            }
        }

es.indices.create(index="rep_dr")
es.indices.put_mapping(index="rep_dr", doc_type="review", body=body_d)'''

####Main Link #####
# the final link will be main_l + name of doc + end_l
# name of doc = Dr_First_Last
main_l = 'http://www.vitals.com/doctors/'
end_l = '/reviews'

#Load+process data
df = pd.read_csv('~/Grapevine_app/final_grapevine/final_list.csv/Summary-Table1.csv', header=0)
#df = pd.read_csv('prov_list.csv', header=0, names=cols)
df = df.drop(df.columns[[0,1,2,5]], axis=1)
df = df.rename(columns={'Last Name':'LAST', 'First Name':'FIRST'})
df = df[pd.notnull(df['FIRST'])]
df = df[pd.notnull(df['LAST'])]
FIRST = list(df["FIRST"])
FIRST = map(str.title,FIRST)
LAST = list(df["LAST"])
LAST = map(str.title, LAST)

entity = "Doctor"

#Txt decoding and clean-up
reload(sys)
sys.setdefaultencoding("utf-8")
trans = string.maketrans(string.punctuation, ' '*len(string.punctuation))

#star dictionary translation
star_dic = {"one":1, "two":2, "three":3, "four":4, "five":5}

def review_scraper(link):
    page = requests.get(link)
    tree = html.fromstring(page.content)
    rating = tree.xpath('//div[@class="rating"]//span//ul[@class="score star medium"]//li/@class')
    stars = []
    for x in rating:
        new_x = x.replace('current','').strip()
        stars.append(star_dic[new_x])
    dates = tree.xpath('//span[@class="date c_date dtreviewed"]//span/@title')
    reviews = tree.xpath('//p[@class="description"]/text()')
    #reviews = [x.decode('unicode_escape').encode('ascii','ignore').translate(trans).replace('\r','').replace('\n','').strip() for x in reviews]
    reviews = [x.decode('unicode_escape').encode('ascii','ignore').replace('\r','').replace('\n','').strip() for x in reviews]
    address = tree.xpath('//span[@itemprop="streetAddress"]//span[@class="addr_line"]/text()')
    city = tree.xpath('//span[@itemprop="addressLocality"]/text()')
    state = tree.xpath('//span[@itemprop="addressRegion"]/text()')
    zipcode = tree.xpath('//span[@itemprop="postalCode"]/text()')
    complete_address = address + city + state
    complete_address = [x.strip() + ', ' for x in complete_address]
    complete_address = complete_address + zipcode
    s = ''
    for x in complete_address:
        s += x
    try:
        geocode_result = gmaps.geocode(s)
        try:
            lat = geocode_result[0]['geometry']['location']['lat']
            lon = geocode_result[0]['geometry']['location']['lng']
        except:
            lat = None
            lon = None
    except googlemaps.exceptions.HTTPError:
        lat = None
        lon = None
    return stars, dates, reviews, lat, lon

def page_tester(link):
    page = requests.get(link)
    tree = html.fromstring(page.content)
    error_pg = tree.xpath('//div[@class="container error"]/@data-error')
    if error_pg == []:
        dummy_var = False
    else:
        dummy_var = True
    return dummy_var


###############
for i in range(len(FIRST)):
    link = main_l + 'Dr_' + FIRST[i] + '_' + LAST[i] + end_l
    pg_tst = page_tester(link)
    time.sleep(1.0)
    if pg_tst:
        pass
    else:
        stars, dts, txt,lat,lon = review_scraper(link)
        name = FIRST[i] + ' ' + LAST[i]
        time.sleep(1.0)
        for p in zip(txt,stars,dts):
            bd = {"name":name, "entity":entity, "text": p[0],"link":link, "date": p[2], "stars": p[1],"location":{"lat":lat,"lon":lon}}
            es.index(index=index, doc_type='review', body= bd)




