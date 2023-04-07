import tarfile
import json
import re
import pandas as pd
from mpi4py import MPI
from datetime import datetime
from collections import Counter
from collections import defaultdict
from mergedeep import merge
import sys

def find_gcc(place_split):
    gcc = None
    if len(place_split) < 2:
        gcc = None
    else:
        if place_split[0] in sal_data.keys():
            gcc = sal_data[place_split[0]]['gcc']
        else:
            if place_split[1] in state_code.keys():
                place_split[0] = place_split[0]+ ' ' + state_code[place_split[1]]
            if place_split[0] in sal_data.keys():
                gcc = sal_data[place_split[0]]['gcc']  
    return gcc


def from_gcc(gcc):
    if gcc[1] == 'g'or gcc[1] == 'a'or gcc[1] == 'o':
        return True
    return False


def add_to_num_tweets(gcc,num_tweets):
    if gcc in num_tweets.keys():
        num_tweets[gcc] += 1
    else:
        num_tweets[gcc] = 1
    return num_tweets

def num_location(tweeter):
    num = None
    gcc = None
    sorted_location = ''
    # sorted_tweeter = dict(sorted(tweeter.items(), key=lambda x:x[1],reverse=True))
    for state in tweeter.keys():
        gcc = state[1:]
        num = str(tweeter.get(state))
        sorted_location = sorted_location + num+ gcc + ","
    return sorted_location[:-1]    


def dict_length(d):
    return len(d.keys())

def dict_sum(d):
    return sum(d.values())

def process(chunk_data):
    num_tweets = Counter()
    most_tweets = Counter()
    tweets_loc = Counter()
    tweeter = defaultdict(Counter)
    for element in chunk_data:

        # Finding the number of tweets in each capital city
        place_split = re.split('[,-]+', element["includes"]["places"][0]["full_name"])
        place_split[0] = place_split[0].lower()

        gcc = find_gcc(place_split)

        if gcc != None and from_gcc(gcc):
            num_tweets[gcc] += 1

        most_tweets[element["data"]["author_id"]] += 1

        if gcc != None and from_gcc(gcc):
            if element["data"]["author_id"] not in tweeter.keys():
                tweeter[element["data"]["author_id"]] = {}

            tweets_loc = tweeter.get(element["data"]["author_id"])
            tweets_loc = add_to_num_tweets(gcc, tweets_loc)
    return num_tweets,most_tweets,tweeter


twitterpath = 'bigTwitter.json'
salpath = 'sal.json'

# twitterpath = 'Data/twitter-data-small.json'
# salpath = 'Data/sal.json'


state_code = {" New South Wales":"(nsw)", " Victoria":"(vic.)", " Queensland":"(qld)", " South Australia":"(sa)", " Western Australia":"(wa)", " Tasmania":"(tas.)", " Northern Territory":"(nt)", " Australian Capital Territory":"(act)"}  

f = open(twitterpath, 'r')
jsonfile = f.read()
data = json.loads(jsonfile)
f_sal = open(salpath, 'r')
sal_json = f_sal.read()
sal_data = json.loads(sal_json)

comm = MPI.COMM_WORLD
processor_pos = comm.Get_rank()
total_processors = comm.Get_size()

START_TIME = datetime.now()
END_TIME = None


print("Tweeter processor running on rank " + str(processor_pos) + " out of " + str(
    total_processors) + " cores")

if processor_pos == 0:
    data_chunk_size = round(len(data) / total_processors)
    chunks = []
    print("Num of processor", total_processors)
    for n in range(total_processors):
        local_start = n*data_chunk_size
        local_end = n*data_chunk_size + data_chunk_size
        chunks.append([data[local_start:local_end]])
else:
    chunks = None

comm.Barrier()
chunk_per_process = comm.scatter(chunks, root=0)
num_tweets_result ,most_tweets_result ,tweeter_result  = process(chunk_per_process[0])

if processor_pos != 0:
    END_TIME = datetime.now()
    print("Execution time on core with rank " + str(processor_pos) + " was: " + str(
        END_TIME - START_TIME))

num_tweets_result = comm.gather(num_tweets_result , root=0)
most_tweets_result = comm.gather(most_tweets_result , root=0)
tweeter_result = comm.gather(tweeter_result , root=0)

comm.Barrier()

if processor_pos == 0:
    num_tweets = Counter()
    most_tweets = Counter()
    tweeter = defaultdict(Counter)
    for n in range(total_processors):
        num_tweets += num_tweets_result[n]
        most_tweets += most_tweets_result[n]
        tweeter = merge(tweeter,tweeter_result[n])

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
    print("==============================================")
    # Converting the tweeter dictionary into a pandas dataframe
    top_tweeter_loc = pd.DataFrame(tweeter.items())
    top_tweeter_loc = top_tweeter_loc.rename({0: 'Author Id', 1: 'Tweets Location'}, axis=1)

    top_tweeter_loc['Sum of Unique City'] = top_tweeter_loc['Tweets Location'].apply(dict_length)
    top_tweeter_loc['Sum of Total Tweets'] = top_tweeter_loc['Tweets Location'].apply(dict_sum)
    top_tweeter_loc['Sorted location'] = top_tweeter_loc['Tweets Location'].apply(num_location)
    top_tweeter_loc = top_tweeter_loc.sort_values(by=['Sum of Unique City','Sum of Total Tweets'] , ascending=False)

    top_tweeter_loc['Number of Unique City Locations and #Tweets'] = top_tweeter_loc.apply(lambda row: f"{row['Sum of Unique City']}(#{row['Sum of Total Tweets']}tweets-{row['Sorted location']})", axis=1)
    top_tweeter_loc = top_tweeter_loc.drop(['Sum of Unique City','Sum of Total Tweets', 'Tweets Location', 'Sorted location'], axis=1)
    top_tweeter_loc = top_tweeter_loc.head(10).reset_index(drop = True)

    print(top_tweeter_loc)

    END_TIME = datetime.now()
    print("Total execution time was: " + str(END_TIME - START_TIME))
MPI.Finalize

