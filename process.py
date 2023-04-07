import tarfile
import json
import re
import pandas as pd
  
 # find what state the region is in and adds 1 to the num_tweets dictionary if its in a capital city
def add_to_num_tweets(place_split, num_tweets):
    gcc = sal_data[place_split[0]]['gcc']
    if gcc[1] == 'g'or gcc[1] == 'a'or gcc[1] == 'o':
        if gcc in num_tweets.keys():
            num_tweets[gcc] += 1
        else:
            num_tweets[gcc] = 1 
    return num_tweets

# opening the .tar.gz files
f1 = tarfile.open('twitter-data-small.json.tar.gz')
f2 = tarfile.open('sal.json.tar.gz')
f1.extractall('./Data')
f2.extractall('./Data')
f1.close()
f2.close()

twitterpath = 'Data/twitter-data-small.json'
salpath = 'Data/sal.json'

num_tweets = {}
state_code = {" New South Wales":"(nsw)", " Victoria":"(vic.)", " Queensland":"(qld)", " South Australia":"(sa)", " Western Australia":"(wa)", " Tasmania":"(tas.)", " Northern Territory":"(nt)", " Australian Capital Territory":"(act)"}  
most_tweets = {}

# Loading in the data
f = open(twitterpath, 'r')
jsonfile = f.read()
data = json.loads(jsonfile)
f_sal = open(salpath, 'r')
sal_json = f_sal.read()
sal_data = json.loads(sal_json)

# Parsing through each of the tweets 
for element in data:
    # Finding the number of tweets in each capital city
    place_split = re.split('[,-]+', element["includes"]["places"][0]["full_name"])
    # print(place_split)
    place_split[0] = place_split[0].lower()
    
    ####  Assume rural area world length > 2??, ['Airlie Beach ', ' Cannonvale', ' Queensland']
    ####  Check rural area in two places. len > 2 && add_to_num_tweets(if gcc[1] == 'g'or gcc[1] == 'a'or gcc[1] == 'o':)
    if len(place_split) == 2: # if there isnt a corresponding state than its ambiguous and thus removed
        if place_split[0] in sal_data.keys():
            num_tweets = add_to_num_tweets(place_split, num_tweets)
        else:
            if place_split[1] in state_code.keys():
                place_split[0] = place_split[0]+ ' ' + state_code[place_split[1]]
            if place_split[0] in sal_data.keys():
                num_tweets = add_to_num_tweets(place_split, num_tweets)
    
    # Finding how many tweets each person has tweeted
    if element["data"]["author_id"] in most_tweets.keys():
        most_tweets[element["data"]["author_id"]] += 1
    else: 
        most_tweets[element["data"]["author_id"]] = 1

# Converting the tweet location dictionary into a pandas dataframe
top_tweet_loc = pd.DataFrame(num_tweets.items())
top_tweet_loc = top_tweet_loc.rename({0: 'Greater Capital City', 1: 'Number of Tweets Made'}, axis=1)
top_tweet_loc = top_tweet_loc.sort_values(by='Number of Tweets Made', ascending=False)
print(top_tweet_loc)
print("==============================================")

# Converting the tweet id dictionary into a pandas dataframe
top_tweeters = pd.DataFrame(most_tweets.items())
top_tweeters = top_tweeters.rename({0: 'Author Id', 1: 'Number of Tweets Made'}, axis=1)
top_tweeters = top_tweeters.sort_values(by='Number of Tweets Made', ascending=False)
top_tweeters = top_tweeters.head(10).reset_index(drop = True)
print(top_tweeters)