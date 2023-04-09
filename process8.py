import json
import re
import pandas as pd
from mpi4py import MPI
from datetime import datetime
from collections import defaultdict
import os
import ijson


def smallerChunk(begin,size,splitBatch = 1024) :
    listBatch = []
    with open(TWITTERPATH, 'rb') as f:
        f.seek(begin)
        lastPointer = f.tell()
        #print(lastPointer)
        while lastPointer < begin + size :
            beginPointer = lastPointer
            f.seek(beginPointer + splitBatch)
            line = f.readline()
            check = line.startswith(b'  }')
            while not check:
                line = f.readline()
                #print("====================\n",line,"\n========================")
                check = line.startswith(b'  }')
                if f.tell() > begin + size  :
                    lastPointer = begin + size
                    break
                lastPointer = f.tell()
            #print("begin :",beginPointer,", last pointer :",lastPointer,", the length:",lastPointer-beginPointer,", the size:",size,", the end:",begin + size)
            listBatch.append((beginPointer,lastPointer-beginPointer))
            lastPointer += 1
    return listBatch

def readFile(chunk):
    df = pd.DataFrame()
    (start, end) = chunk
    with open(TWITTERPATH, 'rb') as f:
        for fileStart, FileSize in smallerChunk(start, end):
            try :
                f.seek(fileStart)
                line = f.read(FileSize).decode('utf-8').strip()
                #
                # print('============')
                if line[-1] == ",":
                    line = line[:-1]
                if line[-1] == "]":
                    line = line[:-1]
                line = "[" + line + "]"
                # print(line)
                objects = ijson.items(line, 'item')
                for o in objects:
                    twit_id = o['_id']
                    author_id = o['data']['author_id']
                    place_split = re.split('[,-]+', o["includes"]["places"][0]["full_name"])
                    place_split[0] = place_split[0].lower()
                    gcc = find_gcc(place_split)
                    new_tweet = pd.DataFrame({'_id': twit_id, 'author_id': author_id, 'gcc': gcc}, index=[0])
                    df = pd.concat([new_tweet, df.loc[:]]).reset_index(drop=True)
            except :
                continue

    return df

def split_file(file_size_processor,file_size_total):
    with open(TWITTERPATH, 'rb') as f:
        end_pointer = f.tell()
        chunks = []
        while end_pointer < file_size_total:

            start_pointer = end_pointer + 1

            f.seek(start_pointer + file_size_processor)
            line = f.readline()
            end_of_single_twit = line.startswith(b'  }')
            while not end_of_single_twit :
                line = f.readline()
                end_of_single_twit = line.startswith(b'  }')
                end_pointer = f.tell()
                if end_pointer > file_size_total:
                    end_pointer = file_size_total
                    break

            chunks.append((start_pointer,end_pointer-start_pointer))

    return chunks

def add_to_num_tweets(gcc,num_tweets):
    if gcc in num_tweets.keys():
        num_tweets[gcc] += 1
    else:
        num_tweets[gcc] = 1
    return num_tweets

def from_gcc(gcc):
    if gcc[1] == 'g'or gcc[1] == 'a'or gcc[1] == 'o':
        return True
    return False


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


TWITTERPATH = 'bigTwitter.json'
SALPATH = 'sal.json'

state_code = {" New South Wales":"(nsw)", " Victoria":"(vic.)", " Queensland":"(qld)", " South Australia":"(sa)", " Western Australia":"(wa)", " Tasmania":"(tas.)", " Northern Territory":"(nt)", " Australian Capital Territory":"(act)"}
gcc_list = ['1gsyd', '2gmel', '3gbri', '4gade', '5gper', '6ghob', '7gdar', '8acte']

f_sal = open(SALPATH, 'r')
sal_json = f_sal.read()
sal_data = json.loads(sal_json)

COMM = MPI.COMM_WORLD
RANK = COMM.Get_rank()
SIZE = COMM.Get_size()

START_TIME = datetime.now()
END_TIME = None


print("Tweeter processor running on rank " + str(RANK) + " out of " + str(
    SIZE) + " cores")
chunks = []

if RANK == 0:

    df = pd.DataFrame()
    with open(TWITTERPATH, "r") as f:

        objects = ijson.items(f, 'item')

        for o in objects:
            file_size_total = os.path.getsize(TWITTERPATH)
            file_size_processor = file_size_total // SIZE
            chunks = split_file(file_size_processor, file_size_total)
else:
    chunks = None

COMM.Barrier()
chunk_per_process = COMM.scatter(chunks, root=0)

data_per_chunk = readFile(chunk_per_process)
num_tweets = data_per_chunk.groupby('gcc').size().reset_index()
num_tweets = num_tweets[num_tweets['gcc'].isin(gcc_list)].reset_index(drop = True)
most_tweets = data_per_chunk.groupby('author_id').size().reset_index()
tweeter = data_per_chunk[data_per_chunk['gcc'].isin(gcc_list)].groupby(['author_id', 'gcc']).size()

if RANK != 0:
    END_TIME = datetime.now()
    print("Execution time on core with rank " + str(RANK) + " was: " + str(
        END_TIME - START_TIME))

num_tweets_result = COMM.gather(num_tweets , root=0)
most_tweets_result = COMM.gather(most_tweets , root=0)
tweeter_result = COMM.gather(tweeter , root=0)

COMM.Barrier()

if RANK == 0:
    # Converting the tweet location dictionary into a pandas dataframe
    # top_tweet_loc = pd.DataFrame(num_tweets.items())
    top_tweet_loc = pd.concat(num_tweets_result)
    top_tweet_loc = top_tweet_loc.rename({"gcc": 'Greater Capital City', 0: 'Number of Tweets Made'}, axis=1)
    top_tweet_loc = top_tweet_loc.sort_values('Number of Tweets Made', ascending=False)
    top_tweet_loc.index = top_tweet_loc.index + 1

    print(top_tweet_loc)
    print("==============================================")
    # Converting the tweet id dictionary into a pandas dataframe
    top_tweeters = pd.concat(most_tweets_result)
    top_tweeters = top_tweeters.rename({'Author Id': 'Author Id', 0: 'Number of Tweets Made'}, axis=1)
    top_tweeters = top_tweeters.sort_values(by='Number of Tweets Made', ascending=False)
    top_tweeters = top_tweeters.head(10).reset_index(drop = True)
    top_tweeters.index = top_tweeters.index + 1

    print(top_tweeters)
    print("==============================================")
    # Converting the tweeter dictionary into a pandas dataframe
    top_tweeters_loc = pd.concat(tweeter_result)
    x = top_tweeters_loc.groupby('author_id').size()
    y = defaultdict(lambda: '')
    z = top_tweeters_loc.groupby('author_id').sum()
    for (id, gcc) in dict(top_tweeters_loc):
        y[id] = y[id] + str(dict(top_tweeters_loc)[(id, gcc)]) + gcc[1:] + ','

    for string in y:
        y[string] =y[string][:-1]

    top_tweeter_loc= pd.DataFrame({'Total Tweets':z,
                    'Num Unique City': x,
                     'Location': y })
    top_tweeter_loc['Number of Unique City Locations and #Tweets'] = top_tweeter_loc.apply(lambda row: f"{row['Num Unique City']}(#{row['Total Tweets']}tweets-{row['Location']})", axis=1)
    top_tweeter_loc = top_tweeter_loc.sort_values(by=['Num Unique City','Total Tweets'] , ascending=False)
    top_tweeter_loc = top_tweeter_loc.drop(['Total Tweets','Num Unique City', 'Location'], axis=1)
    top_tweeter_loc = top_tweeter_loc.head(10).reset_index()
    top_tweeter_loc = top_tweeter_loc.rename({'index': 'Author Id'}, axis=1)

    print(top_tweeter_loc)
    END_TIME = datetime.now()
    print("Total execution time was: " + str(END_TIME - START_TIME))
MPI.Finalize

