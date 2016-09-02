import requests
from lxml import html
import sys 
import pandas as pd
import string
import csv
import time
import googlemaps


################geospatial api ###########
gmaps_key = 'AIzaSyDHmGmfTxxapify6Op8jio-Pv4rCVe0pbE'
gmaps = googlemaps.Client(key=gmaps_key)
##########################################


link_csv = open("vitals_pg_link.csv","wb")
open_file_object = csv.writer(link_csv)
open_file_object.writerow(["Name","Location","Address","LAT","LON","Rating","Pg Link"])
link_csv.close()

####Main Link #####
# the final link will be main_l + name of doc + end_l
# name of doc = Dr_First_Last
main_l = 'http://www.vitals.com/doctors/'
end_l = '/reviews'

#Load+process data
df = pd.read_csv('/Users/vonschoeneck/Desktop/Heritage_rep/final_list.csv/Summary-Table1.csv', header=0)
#df = pd.read_csv('', header=0, names=cols)
df = df.drop(df.columns[[0,1,2,5]], axis=1)
df = df.rename(columns={'Last Name':'LAST', 'First Name':'FIRST'})
df = df[pd.notnull(df['FIRST'])]
df = df[pd.notnull(df['LAST'])]
FIRST = list(df["FIRST"])
FIRST = map(str.title,FIRST)
LAST = list(df["LAST"])
LAST = map(str.title, LAST)



#Txt decoding and clean-up
reload(sys)
sys.setdefaultencoding("utf-8")
trans = string.maketrans(string.punctuation, ' '*len(string.punctuation))

#star dictionary translation
star_dic = {"one":1, "two":2, "three":3, "four":4, "five":5}

def review_scraper(link):
    page = requests.get(link)
    tree = html.fromstring(page.content)
    over_stars = tree.xpath('//span[@class="score overview-number"]/text()')
    over_stars = [int(float(x)) for x in over_stars]
    over_stars = list(set(over_stars))
    area = tree.xpath('//address[@itemprop="address"]//span[@class="title"]/text()')
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
        lat = geocode_result[0]['geometry']['location']['lat']
        lon = geocode_result[0]['geometry']['location']['lng']
    except googlemaps.exceptions.TransportError:
        lat = None
        lon = None
    return over_stars, area, lat, lon, s

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
    time.sleep(10.0)
    if pg_tst:
        pass
    else:
        try:
            stars,area, lat,lon,s = review_scraper(link)
            if 'CA' in s:
                name = FIRST[i] + ' ' + LAST[i]
                time.sleep(10.0)
                for p in zip(area,stars):
                    link_csv = open("vitals_pg_link.csv","a")
                    open_file_object = csv.writer(link_csv)
                    open_file_object.writerow([name,p[0],s,lat,lon,p[1],link])
                    link_csv.close()
        except requests.exceptions.ConnectionError:
            pass




