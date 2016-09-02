import requests
from lxml import html
import sys
import string
import pandas as pd
from addict import Dict
from elasticsearch import Elasticsearch
import json
import watson_developer_cloud
from watson_developer_cloud import ToneAnalyzerV3
#import googlemaps

es = Elasticsearch()

#Tone Analyzer credentials:
username = "070a0db0-22c9-4905-93b2-160185a13c28"
password = "SI5yIAsVfGhZ"
##############################

tone_analyzer = ToneAnalyzerV3(username=username,password=password,version='2016-05-19')


#Geocoding to integrate later -https://github.com/googlemaps/google-maps-services-python
#gmaps_key = 'AIzaSyDHmGmfTxxapify6Op8jio-Pv4rCVe0pbE'
#gmaps = googlemaps.Client(key=gmaps_key)
# Geocoding an address
#geocode_result = gmaps.geocode('1600 Amphitheatre Parkway, Mountain View, CA')

index = sys.argv[1]
#index = 'rep'

'''body_d = Dict()
body_d.review.properties.name.type = "string"
body_d.review.properties.name.index = "not_analyzed"
body_d.review.properties.text.type = "string"
body_d.review.properties.text.index = "analyzed"
body_d.review.properties.date.type = "date"
body_d.review.properties.stars.type = "float" '''

body_d = {
        "review": {
            "properties": {
                "name": {"type": "string", "index":"not_analyzed"},
                "entity": {"type":"string", "index":"not_analyzed"},
                "text": {"type": "string", "index": "analyzed"},
                "date": {"type": "date"},
                "stars": {"type": "integer"},
                "link": {"type":"string", "index":"not_analyzed"},
                "location": {
                    "type": "geo_point",
                    "geohash_prefix": True,
                    "geohash_precision":  "1km"}
                }
            }
        }


es.indices.create(index=index)
es.indices.put_mapping(index=index, doc_type="review", body=body_d)



#Txt decoding and clean-up
reload(sys)
sys.setdefaultencoding("utf-8")
trans = string.maketrans(string.punctuation, ' '*len(string.punctuation))



### Names of Companies ###
df = pd.read_csv('yelp_pg_link.csv', header=0)
df = df.drop(df.columns[[1,2]], axis=1)
df = df.drop_duplicates(subset=['Yelp Pg Link'])
name_co = list(df["Name"]) 
#['Regal Medical Group','Lakeside Community Healthcare','Affiliated Doctors of Orange County']

#### Sample Links ####
total_l = list(df["Yelp Pg Link"])
#link1 = 'https://www.yelp.com/biz/regal-medical-group-northridge?sort_by=date_desc'
#link2 = 'https://www.yelp.com/biz/lakeside-community-healthcare-urgent-care-burbank-burbank?sort_by=rating_asc'
#link3 = 'http://www.yelp.com/biz/affiliated-doctors-of-orange-county-garden-grove-2?sort_by=rating_asc'
#total_l = [link1, link2, link3]

#geocoding info
total_lat = list(df["LAT"])
total_long = list(df["LONG"])

##Bounding box helper function##
#def bounding_box(lat, lon):
#    max_lat = 42.040465
#    min_lat = 32.564574
#    min_lon = -122.829548 
#    max_lon = -116.499234 
#if lat_min < lat < lat_max AND long_min < long < long_max:
#    if min_lat < lat < max_lat and min_lon < lon < max_lon:
#        dummy_var = True
#    else:
#        dummy_var = False
#    return dummy_var

entity = 'Clinic'

def review_stars_date(link):
    page = requests.get(link)
    tree = html.fromstring(page.content)
    stars = tree.xpath('//meta[@itemprop="ratingValue"]/@content')
    review_star = stars[1:]
    review_star = map(float,review_star)
    review_star = map(int,review_star)
    review_date = tree.xpath('//meta[@itemprop="datePublished"]/@content')
    return review_star, review_date


def review_scraper(link):
    page = requests.get(link)
    tree = html.fromstring(page.content.replace('<br>',' '))
    review_txt = tree.xpath('//div[@class="review-wrapper"]//div[@class="review-content"]//p/text()')
    #review_txt = [x.decode('unicode_escape').encode('ascii','ignore').translate(trans) for x in review_txt]
    review_txt = [x.decode('unicode_escape').encode('ascii','ignore').replace('\n','') for x in review_txt]
    return review_txt


def page_checker(link):
    page = requests.get(link)
    tree = html.fromstring(page.content)
    page_num = tree.xpath('//div[@class="page-of-pages arrange_unit arrange_unit--fill"]/text()')
    page_num = [x.replace('\n','').strip() for x in page_num]
    page_num = page_num[0].split()
    no_pg =[]
    for x in page_num:
        try:
            no_pg.append(int(x))
        except:
            pass
    if len(list(set(no_pg))) == 1:
        dummy_var = False
    else:
        dummy_var = True
    return dummy_var

####Tone analyzer max sentence###
def max_sentence_id(x):
    max_score = float('-inf')
    max_sentence = ''
    if 'sentences_tone' in x.keys():
        for i in x['sentences_tone']:
            try:
                score, sentence = i['tone_categories'][0]['tones'][0]['score'],i['text']
                if score > max_score:
                    max_score = score
                    max_sentence = sentence
            except IndexError:
                pass
    return str(max_sentence)

for s in range(len(total_l)):
    name = name_co[s]
    link_r = total_l[s]
    lat = total_lat[s]
    lon = total_long[s]
    txt = review_scraper(total_l[s])
    stars, dts =review_stars_date(total_l[s])
    data = zip(txt,stars)
#####Try to input tone data in corresponding txt
    tone_data = []
    for x in txt:
        tone_txt = tone_analyzer.tone(text=x)
        tone_data.append(tone_txt)
    for x in range(len(tone_data)):
        if len(data) == len(tone_data):
            tmp_lst = []
            if 'sentences_tone' in tone_data[x].keys():
                for b in tone_data[x]['sentences_tone']:
                    tmp_lst.append(str(b['text']))
                if data[x][1] < 4:
                    sentence  = max_sentence_id(tone_data[x])
                    ind = tmp_lst.index(sentence)
                    tmp_lst[ind] = '<mark>'+ sentence +'</mark>'
                    text_tone = ' '.join(tmp_lst)
                    txt[x] = text_tone
    dummy = page_checker(total_l[s])
    for p in zip(txt,stars,dts):
        bd = {"name":name, "text": p[0],"entity":entity, "date": p[2], "stars": p[1], "link": link_r,"location":{"lat":lat,"lon":lon}}
        es.index(index=index, doc_type='review', body= bd)
    i = 20
    while dummy:
        spl_link = total_l[s].split('?')
        pg_str = 'start='
        new_link = spl_link[0]+ '?' + pg_str + str(i) + '&' + spl_link[1]
        txt = review_scraper(new_link)
        stars, dts =review_stars_date(new_link)
        data = zip(txt,stars)
        tone_data = []
        for x in txt:
            tone_txt = tone_analyzer.tone(text=x)
            tone_data.append(tone_txt)
        for x in range(len(tone_data)):
            if len(data) == len(tone_data):
                tmp_lst = []
                if 'sentences_tone' in tone_data[x].keys():
                    for b in tone_data[x]['sentences_tone']:
                        tmp_lst.append(str(b['text']))
                    if data[x][1] < 4:
                        sentence  = max_sentence_id(tone_data[x])
                        ind = tmp_lst.index(sentence)
                        tmp_lst[ind] = '<mark>'+ sentence +'</mark>'
                        text_tone = ' '.join(tmp_lst)
                        txt[x] = text_tone
        for p in zip(txt,stars,dts):
            bd = {"name":name, "text": p[0], "entity":entity, "date": p[2], "stars": p[1],"link":new_link,"location":{"lat":lat,"lon":lon}}
            es.index(index=index, doc_type='review', body= bd)
        i+= 20
        dummy = page_checker(new_link)


