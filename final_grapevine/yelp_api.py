import rauth
import time
import csv as csv
import pandas as pd

link_csv = open("yelp_pg_link.csv","wb")
open_file_object = csv.writer(link_csv)
open_file_object.writerow(["Name","Location","Rating","Yelp Pg Link","LAT","LONG"])
link_csv.close()

##################################################################
# input data processing #
address = pd.read_csv('/Users/vonschoeneck/Desktop/Heritage_rep/final_list.csv/Address-Table1.csv', header=0)
summary = pd.read_csv('/Users/vonschoeneck/Desktop/Heritage_rep/final_list.csv/Summary-Table1.csv', header=0)
address= address.drop(address.columns[[0,4,5]],axis=1)
summary = summary.drop(summary.columns[[2,3,4,5]], axis=1)
final_df = pd.merge(address, summary, on='Location', how='inner')
final_df = final_df.drop_duplicates()
name_dic = {"RMG":"Regal Medical Group", "LMG":"Lakeside Medical Group"}
name  = list(final_df['Co'])
name = [name_dic[x] for x in name]
name.append('Affiliated Doctors of Orange County')
addss = list(final_df['Address'])
city_st = list(final_df['City, Sate, Zip'])
final_loc = zip(addss, city_st)
final_loc = [' '.join(x) for x in final_loc]
final_loc.append('Garden Grove, CA')


################################################################


def main():
    stars = []
    links = []
    latitudes = []
    longitudes = []
    link_csv = open("yelp_pg_link.csv","a")
    open_file_object = csv.writer(link_csv)
    for i in range(len(name)):
        params = get_search_parameters(name[i],final_loc[i])
        response = get_results(params)
        link = str(response['businesses'][0]['url'])
        link = link.split('?')[0] + '?sort_by=rating_asc'
        links.append(link)
        star = int(response['businesses'][0]['rating'])
        stars.append(star)
        latitude = response['businesses'][0]['location']['coordinate']['latitude']
        longitude = response['businesses'][0]['location']['coordinate']['longitude']
        latitudes.append(latitude)
        longitudes.append(longitude)
        time.sleep(1.0)
    for i in range(len(name)):
        open_file_object.writerow([name[i],final_loc[i],stars[i],links[i],latitudes[i],longitudes[i]])
    link_csv.close()


def get_search_parameters(name,final_loc):
    params = {}
    params["term"] = name
    params["location"] = final_loc
    return params

def get_results(params):
    consumer_key = 'Yq7dBOlGfiZ5cDATqK4NiQ'
    consumer_secret = '4XyV0YEv326nAwH_USfn95EWZik'
    token = 'Xjz-nLqH0NR0GhSTMGZh_kYGN3mKGonI'
    token_secret = '2Ie0QCj0AGkvxiGsp3vNxZI_a24'

    session = rauth.OAuth1Session(
            consumer_key = consumer_key,
            consumer_secret = consumer_secret,
            access_token = token,
            access_token_secret = token_secret)
    request = session.get("http://api.yelp.com/v2/search",params=params)
    #Transforms the JSON API response into a Python dictionary
    data = request.json()
    session.close()
    
    return data


if __name__=="__main__":
    main()
    
