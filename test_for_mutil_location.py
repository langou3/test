import tarfile
import json
import re
import pandas as pd


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

def sorted_location(tweeter):
    num = None
    gcc = None
    sorted_location = ''
    sorted_tweeter = dict(sorted(tweeter.items(), key=lambda x:x[1],reverse=True))
    for state in sorted_tweeter.keys():
        gcc = state[1:]
        num = str(tweeter.get(state))
        sorted_location = sorted_location + num+ gcc + ","
    return sorted_location[:-1]    


def dict_length(d):
    return len(d.keys())

def dict_sum(d):
    return sum(d.values())


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
tweeter = {}


f = open(twitterpath, 'r')
jsonfile = f.read()
data = json.loads(jsonfile)
f_sal = open(salpath, 'r')
sal_json = f_sal.read()
sal_data = json.loads(sal_json)

# data1 = [{'_id': '1412192485812555778', '_rev': '2-76667587bc37ce06a551b61a9ec36656', 'data': {'author_id': '156677140', 'conversation_id': '1412178746828681221', 'created_at': '2021-07-05T23:31:40.000Z', 'entities': {'annotations': [{'start': 60, 'end': 63, 'probability': 0.4782, 'type': 'Person', 'normalized_text': 'Albo'}], 'mentions': [{'start': 0, 'end': 8, 'username': 'z_p1ngu', 'id': '441036228'}, {'start': 9, 'end': 19, 'username': 'OtherAudi', 'id': '1341641077674057728'}, {'start': 20, 'end': 27, 'username': 'AlboMP', 'id': '254515782'}]}, 'geo': {'place_id': '0073b76548e5984f'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@z_p1ngu @OtherAudi @AlboMP Even this is more detailed than Albo', 'sentiment': 0}, 'includes': {'places': [{'full_name': 'Sydney, New South Wales', 'geo': {'type': 'Feature', 'bbox': [150.520928608, -34.1183470085, 151.343020992, -33.578140996], 'properties': {}}, 'id': '0073b76548e5984f'}]}, 'matching_rules': [{'id': 1412189062442586000, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412192549398192129', '_rev': '2-f4085018897e2d91abe7cfcd55abe340', 'data': {'author_id': '156677140', 'context_annotations': [{'domain': {'id': '10', 'name': 'Person', 'description': 'Named people in the world like Nelson Mandela'}, 'entity': {'id': '1043141608147841024', 'name': 'Scott Morrison', 'description': 'Prime Minister of Australia, Scott Morrison'}}, {'domain': {'id': '11', 'name': 'Sport', 'description': 'Types of sports, like soccer and basketball'}, 'entity': {'id': '847902509750407168', 'name': 'Rugby', 'description': 'Rugby'}}, {'domain': {'id': '12', 'name': 'Sports Team', 'description': 'A sports team organization, like Arsenal and the Boston Celtics'}, 'entity': {'id': '1159931650655973376', 'name': 'Australia', 'description': "Australia Men's Rugby Union Team"}}, {'domain': {'id': '35', 'name': 'Politician', 'description': 'Politicians in the world, like Joe Biden'}, 'entity': {'id': '1043141608147841024', 'name': 'Scott Morrison', 'description': 'Prime Minister of Australia, Scott Morrison'}}], 'conversation_id': '1412192549398192129', 'created_at': '2021-07-05T23:31:55.000Z', 'entities': {'annotations': [{'start': 74, 'end': 84, 'probability': 0.9937, 'type': 'Person', 'normalized_text': 'Mark Latham'}, {'start': 87, 'end': 98, 'probability': 0.9927, 'type': 'Person', 'normalized_text': 'Lyle Shelton'}, {'start': 197, 'end': 205, 'probability': 0.9982, 'type': 'Place', 'normalized_text': 'Australia'}], 'mentions': [{'start': 18, 'end': 34, 'username': 'ScottMorrisonMP', 'id': '34116377'}, {'start': 52, 'end': 63, 'username': 'LiberalAus', 'id': '28739612'}, {'start': 105, 'end': 113, 'username': 'ACLobby', 'id': '51402293'}], 'urls': [{'start': 208, 'end': 231, 'url': 'https://t.co/yj9FLkwTf4', 'expanded_url': 'https://twitter.com/pinknews/status/1411930843766001672', 'display_url': 'twitter.com/pinknews/statu…'}]}, 'geo': {'place_id': '0073b76548e5984f'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': 'It is people like @ScottMorrisonMP, a whole swag of @liberalaus pollies,  Mark Latham, Lyle Shelton, The @ACLobby et al., that contribute to keeping male professional sportspeople in the closet in Australia. https://t.co/yj9FLkwTf4', 'sentiment': 0.05714285714285714}, 'includes': {'places': [{'full_name': 'Sydney, New South Wales', 'geo': {'type': 'Feature', 'bbox': [150.520928608, -34.1183470085, 151.343020992, -33.578140996], 'properties': {}}, 'id': '0073b76548e5984f'}]}, 'matching_rules': [{'id': 1412189062442586000, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412198005751513089', '_rev': '2-a9f70fe5cd4e3e20aae28ff7d78170fb', 'data': {'author_id': '156677140', 'conversation_id': '1412197496697221124', 'created_at': '2021-07-05T23:53:36.000Z', 'entities': {'annotations': [{'start': 70, 'end': 74, 'probability': 0.9159, 'type': 'Person', 'normalized_text': 'Brent'}], 'mentions': [{'start': 0, 'end': 12, 'username': 'brentread_7', 'id': '160813248'}, {'start': 13, 'end': 25, 'username': 'NRL_Dragons', 'id': '96717383'}]}, 'geo': {'place_id': '0180d6313aa616b2'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@brentread_7 @NRL_Dragons Do you know what time the board are meeting Brent?', 'sentiment': 0}, 'includes': {'places': [{'full_name': 'Wollongong, New South Wales', 'geo': {'type': 'Feature', 'bbox': [150.743944832, -34.628601669, 150.973866016, -34.2513407335], 'properties': {}}, 'id': '0180d6313aa616b2'}]}, 'matching_rules': [{'id': 1412189062442586000, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412186290687152128', '_rev': '2-3ba24b3fdaa2c1dfc258a50f55b273ac', 'data': {'author_id': '156677140', 'conversation_id': '1412183811224346626', 'created_at': '2021-07-05T23:07:03.000Z', 'entities': {'annotations': [{'start': 99, 'end': 108, 'probability': 0.9884, 'type': 'Person', 'normalized_text': 'Rob Harris'}, {'start': 187, 'end': 194, 'probability': 0.6368, 'type': 'Place', 'normalized_text': 'Ballarat'}, {'start': 200, 'end': 203, 'probability': 0.902, 'type': 'Person', 'normalized_text': 'Shep'}], 'mentions': [{'start': 0, 'end': 15, 'username': 'matt__nicholls', 'id': '497806561'}, {'start': 16, 'end': 28, 'username': 'VinceRugari', 'id': '18885134'}]}, 'geo': {'place_id': '01864a8a64df9dc4'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@matt__nicholls @VinceRugari Twice made it to the podium there, I’m told, for a gig. The excellent Rob Harris got me on one occasion. And I managed to blow up my old bomb halfway between Ballarat and Shep on the second occasion. Had to go to a farm to use the phone to advise I’d be a day late for the IV.', 'sentiment': 0}, 'includes': {'places': [{'full_name': 'Melbourne, Victoria', 'geo': {'type': 'Feature', 'bbox': [144.593741856, -38.433859306, 145.512528832, -37.5112737225], 'properties': {}}, 'id': '01864a8a64df9dc4'}]}, 'matching_rules': [{'id': 1412184603519971300, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412186464822075394', '_rev': '2-82c77b797c0e2779a5c7058f4ff52f5b', 'data': {'author_id': '156677140', 'conversation_id': '1411664114749870088', 'created_at': '2021-07-05T23:07:45.000Z', 'entities': {'hashtags': [{'start': 37, 'end': 41, 'tag': 'TNL'}], 'mentions': [{'start': 0, 'end': 8, 'username': 'prudinx', 'id': '613264547'}, {'start': 9, 'end': 24, 'username': 'RichardYabsley', 'id': '1597408068'}, {'start': 25, 'end': 36, 'username': 'stevie_bro', 'id': '412037761'}]}, 'geo': {'place_id': '01864a8a64df9dc4'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@prudinx @RichardYabsley @stevie_bro #TNL have ICAC with real teeth⭐️', 'sentiment': 0}, 'includes': {'places': [{'full_name': 'Melbourne, Victoria', 'geo': {'type': 'Feature', 'bbox': [144.593741856, -38.433859306, 145.512528832, -37.5112737225], 'properties': {}}, 'id': '01864a8a64df9dc4'}]}, 'matching_rules': [{'id': 1412184603519971300, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412189302352748548', '_rev': '2-bc456585b950b0813f1b7329ca0d40a1', 'data': {'author_id': '156677140', 'conversation_id': '1412189302352748548', 'created_at': '2021-07-05T23:19:01.000Z', 'entities': {'urls': [{'start': 29, 'end': 52, 'url': 'https://t.co/0MLSrQtnO0', 'expanded_url': 'https://www.instagram.com/p/CQ9rM0SlLq3/?utm_medium=twitter', 'display_url': 'instagram.com/p/CQ9rM0SlLq3/…', 'images': [{'url': 'https://pbs.twimg.com/news_img/1412189305511022593/5dSsV4tZ?format=jpg&name=orig', 'width': 1080, 'height': 1080}, {'url': 'https://pbs.twimg.com/news_img/1412189305511022593/5dSsV4tZ?format=jpg&name=150x150', 'width': 150, 'height': 150}], 'status': 200, 'title': 'BeerCo.com.au on Instagram: “Hoppy Tuesday #Brewers 🍻 some exciting NEW! #Hops just landed into the Shop available online at BeerCo.com.au in sizes from 100g to 1Kg…”', 'unwound_url': 'https://www.instagram.com/p/CQ9rM0SlLq3/?utm_medium=twitter'}]}, 'geo': {'coordinates': {'type': 'Point', 'coordinates': [144.85776, -37.73341]}, 'place_id': '01864a8a64df9dc4'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': 'Just posted a photo @ BeerCo https://t.co/0MLSrQtnO0', 'sentiment': 0}, 'includes': {'places': [{'full_name': 'Melbourne, Victoria', 'geo': {'type': 'Feature', 'bbox': [144.593741856, -38.433859306, 145.512528832, -37.5112737225], 'properties': {}}, 'id': '01864a8a64df9dc4'}]}, 'matching_rules': [{'id': 1412189062442586000, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412185186096189448', '_rev': '2-2e8e9a7aa792211f78e7e3de26ed7ac5', 'data': {'author_id': '156677140', 'conversation_id': '1412185186096189448', 'created_at': '2021-07-05T23:02:40.000Z', 'entities': {'annotations': [{'start': 16, 'end': 20, 'probability': 0.9941, 'type': 'Person', 'normalized_text': 'Sarah'}], 'mentions': [{'start': 0, 'end': 12, 'username': 'DocStanford', 'id': '2912577133'}]}, 'geo': {'place_id': '01e4b0c84959d430'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@DocStanford Hi Sarah, I’d like to DM you. Please can you follow me back. 🙏🏻', 'sentiment': 0.2}, 'includes': {'places': [{'full_name': 'Canberra, Australian Capital Territory', 'geo': {'type': 'Feature', 'bbox': [148.9959216, -35.480260417, 149.263643456, -35.147699163], 'properties': {}}, 'id': '01e4b0c84959d430'}]}, 'matching_rules': [{'id': 1412184603519971300, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412185186096189448', '_rev': '2-2e8e9a7aa792211f78e7e3de26ed7ac5', 'data': {'author_id': '77140', 'conversation_id': '1412185186096189448', 'created_at': '2021-07-05T23:02:40.000Z', 'entities': {'annotations': [{'start': 16, 'end': 20, 'probability': 0.9941, 'type': 'Person', 'normalized_text': 'Sarah'}], 'mentions': [{'start': 0, 'end': 12, 'username': 'DocStanford', 'id': '2912577133'}]}, 'geo': {'place_id': '01e4b0c84959d430'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@DocStanford Hi Sarah, I’d like to DM you. Please can you follow me back. 🙏🏻', 'sentiment': 0.2}, 'includes': {'places': [{'full_name': 'Canberra, Australian Capital Territory', 'geo': {'type': 'Feature', 'bbox': [148.9959216, -35.480260417, 149.263643456, -35.147699163], 'properties': {}}, 'id': '01e4b0c84959d430'}]}, 'matching_rules': [{'id': 1412184603519971300, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412185186096189448', '_rev': '2-2e8e9a7aa792211f78e7e3de26ed7ac5', 'data': {'author_id': '77140', 'conversation_id': '1412185186096189448', 'created_at': '2021-07-05T23:02:40.000Z', 'entities': {'annotations': [{'start': 16, 'end': 20, 'probability': 0.9941, 'type': 'Person', 'normalized_text': 'Sarah'}], 'mentions': [{'start': 0, 'end': 12, 'username': 'DocStanford', 'id': '2912577133'}]}, 'geo': {'place_id': '01e4b0c84959d430'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@DocStanford Hi Sarah, I’d like to DM you. Please can you follow me back. 🙏🏻', 'sentiment': 0.2}, 'includes': {'places': [{'full_name': 'Canberra, Australian Capital Territory', 'geo': {'type': 'Feature', 'bbox': [148.9959216, -35.480260417, 149.263643456, -35.147699163], 'properties': {}}, 'id': '01e4b0c84959d430'}]}, 'matching_rules': [{'id': 1412184603519971300, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412185186096189448', '_rev': '2-2e8e9a7aa792211f78e7e3de26ed7ac5', 'data': {'author_id': '156670', 'conversation_id': '1412185186096189448', 'created_at': '2021-07-05T23:02:40.000Z', 'entities': {'annotations': [{'start': 16, 'end': 20, 'probability': 0.9941, 'type': 'Person', 'normalized_text': 'Sarah'}], 'mentions': [{'start': 0, 'end': 12, 'username': 'DocStanford', 'id': '2912577133'}]}, 'geo': {'place_id': '01e4b0c84959d430'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@DocStanford Hi Sarah, I’d like to DM you. Please can you follow me back. 🙏🏻', 'sentiment': 0.2}, 'includes': {'places': [{'full_name': 'Canberra, Australian Capital Territory', 'geo': {'type': 'Feature', 'bbox': [148.9959216, -35.480260417, 149.263643456, -35.147699163], 'properties': {}}, 'id': '01e4b0c84959d430'}]}, 'matching_rules': [{'id': 1412184603519971300, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412185186096189448', '_rev': '2-2e8e9a7aa792211f78e7e3de26ed7ac5', 'data': {'author_id': '177140', 'conversation_id': '1412185186096189448', 'created_at': '2021-07-05T23:02:40.000Z', 'entities': {'annotations': [{'start': 16, 'end': 20, 'probability': 0.9941, 'type': 'Person', 'normalized_text': 'Sarah'}], 'mentions': [{'start': 0, 'end': 12, 'username': 'DocStanford', 'id': '2912577133'}]}, 'geo': {'place_id': '01e4b0c84959d430'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@DocStanford Hi Sarah, I’d like to DM you. Please can you follow me back. 🙏🏻', 'sentiment': 0.2}, 'includes': {'places': [{'full_name': 'Canberra, Australian Capital Territory', 'geo': {'type': 'Feature', 'bbox': [148.9959216, -35.480260417, 149.263643456, -35.147699163], 'properties': {}}, 'id': '01e4b0c84959d430'}]}, 'matching_rules': [{'id': 1412184603519971300, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412186290687152128', '_rev': '2-3ba24b3fdaa2c1dfc258a50f55b273ac', 'data': {'author_id': '167140', 'conversation_id': '1412183811224346626', 'created_at': '2021-07-05T23:07:03.000Z', 'entities': {'annotations': [{'start': 99, 'end': 108, 'probability': 0.9884, 'type': 'Person', 'normalized_text': 'Rob Harris'}, {'start': 187, 'end': 194, 'probability': 0.6368, 'type': 'Place', 'normalized_text': 'Ballarat'}, {'start': 200, 'end': 203, 'probability': 0.902, 'type': 'Person', 'normalized_text': 'Shep'}], 'mentions': [{'start': 0, 'end': 15, 'username': 'matt__nicholls', 'id': '497806561'}, {'start': 16, 'end': 28, 'username': 'VinceRugari', 'id': '18885134'}]}, 'geo': {'place_id': '01864a8a64df9dc4'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': '@matt__nicholls @VinceRugari Twice made it to the podium there, I’m told, for a gig. The excellent Rob Harris got me on one occasion. And I managed to blow up my old bomb halfway between Ballarat and Shep on the second occasion. Had to go to a farm to use the phone to advise I’d be a day late for the IV.', 'sentiment': 0}, 'includes': {'places': [{'full_name': 'Melbourne, Victoria', 'geo': {'type': 'Feature', 'bbox': [144.593741856, -38.433859306, 145.512528832, -37.5112737225], 'properties': {}}, 'id': '01864a8a64df9dc4'}]}, 'matching_rules': [{'id': 1412184603519971300, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}
# ,{'_id': '1412192549398192129', '_rev': '2-f4085018897e2d91abe7cfcd55abe340', 'data': {'author_id': '157740', 'context_annotations': [{'domain': {'id': '10', 'name': 'Person', 'description': 'Named people in the world like Nelson Mandela'}, 'entity': {'id': '1043141608147841024', 'name': 'Scott Morrison', 'description': 'Prime Minister of Australia, Scott Morrison'}}, {'domain': {'id': '11', 'name': 'Sport', 'description': 'Types of sports, like soccer and basketball'}, 'entity': {'id': '847902509750407168', 'name': 'Rugby', 'description': 'Rugby'}}, {'domain': {'id': '12', 'name': 'Sports Team', 'description': 'A sports team organization, like Arsenal and the Boston Celtics'}, 'entity': {'id': '1159931650655973376', 'name': 'Australia', 'description': "Australia Men's Rugby Union Team"}}, {'domain': {'id': '35', 'name': 'Politician', 'description': 'Politicians in the world, like Joe Biden'}, 'entity': {'id': '1043141608147841024', 'name': 'Scott Morrison', 'description': 'Prime Minister of Australia, Scott Morrison'}}], 'conversation_id': '1412192549398192129', 'created_at': '2021-07-05T23:31:55.000Z', 'entities': {'annotations': [{'start': 74, 'end': 84, 'probability': 0.9937, 'type': 'Person', 'normalized_text': 'Mark Latham'}, {'start': 87, 'end': 98, 'probability': 0.9927, 'type': 'Person', 'normalized_text': 'Lyle Shelton'}, {'start': 197, 'end': 205, 'probability': 0.9982, 'type': 'Place', 'normalized_text': 'Australia'}], 'mentions': [{'start': 18, 'end': 34, 'username': 'ScottMorrisonMP', 'id': '34116377'}, {'start': 52, 'end': 63, 'username': 'LiberalAus', 'id': '28739612'}, {'start': 105, 'end': 113, 'username': 'ACLobby', 'id': '51402293'}], 'urls': [{'start': 208, 'end': 231, 'url': 'https://t.co/yj9FLkwTf4', 'expanded_url': 'https://twitter.com/pinknews/status/1411930843766001672', 'display_url': 'twitter.com/pinknews/statu…'}]}, 'geo': {'place_id': '0073b76548e5984f'}, 'lang': 'en', 'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0}, 'text': 'It is people like @ScottMorrisonMP, a whole swag of @liberalaus pollies,  Mark Latham, Lyle Shelton, The @ACLobby et al., that contribute to keeping male professional sportspeople in the closet in Australia. https://t.co/yj9FLkwTf4', 'sentiment': 0.05714285714285714}, 'includes': {'places': [{'full_name': 'Sydney, New South Wales', 'geo': {'type': 'Feature', 'bbox': [150.520928608, -34.1183470085, 151.343020992, -33.578140996], 'properties': {}}, 'id': '0073b76548e5984f'}]}, 'matching_rules': [{'id': 1412189062442586000, 'tag': 'Australia-based users or Australia-located tweets, but no re-tweets'}]}

# ]



for element in data:
    # Finding the number of tweets in each capital city
    place_split = re.split('[,-]+', element["includes"]["places"][0]["full_name"])
    place_split[0] = place_split[0].lower()

    gcc = find_gcc(place_split)

    if gcc != None and from_gcc(gcc):
        num_tweets = add_to_num_tweets(gcc,num_tweets)

    if element["data"]["author_id"] in most_tweets.keys():
        most_tweets[element["data"]["author_id"]] += 1
    else: 
        most_tweets[element["data"]["author_id"]] = 1

    if gcc != None and from_gcc(gcc):
        if element["data"]["author_id"] not in tweeter.keys():
            tweeter[element["data"]["author_id"]] = {}

        tweets_loc = tweeter.get(element["data"]["author_id"])
        tweets_loc = add_to_num_tweets(gcc, tweets_loc)   



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

top_tweeter_loc = pd.DataFrame(tweeter.items())
top_tweeter_loc = top_tweeter_loc.rename({0: 'Author Id', 1: 'Tweets Location'}, axis=1)

top_tweeter_loc['Sum of Unique City'] = top_tweeter_loc['Tweets Location'].apply(dict_length)
top_tweeter_loc['Sum of Total Tweets'] = top_tweeter_loc['Tweets Location'].apply(dict_sum)
top_tweeter_loc['Sorted location'] = top_tweeter_loc['Tweets Location'].apply(sorted_location)
top_tweeter_loc = top_tweeter_loc.sort_values(by=['Sum of Unique City','Sum of Total Tweets'] , ascending=False)

top_tweeter_loc['Number of Unique City Locations and #Tweets'] = top_tweeter_loc.apply(lambda row: f"{row['Sum of Unique City']}(#{row['Sum of Total Tweets']}tweets-{row['Sorted location']})", axis=1)
top_tweeter_loc = top_tweeter_loc.drop(['Sum of Unique City','Sum of Total Tweets', 'Tweets Location', 'Sorted location'], axis=1)
top_tweeter_loc = top_tweeter_loc.head(10).reset_index(drop = True)

print(top_tweeter_loc)
