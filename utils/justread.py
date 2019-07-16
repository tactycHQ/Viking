import json
from twitter_api import GetTwitter
import tweepy as tw
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)


def getJustRead():
    '''Queries Twitter API and gets tweet for just watched string'''

    content_string = '("just read" OR "reading") AND book'
    filter_string = "-filter:retweets -filter:links -filter:replies"
    excl_string = ""
    jw_query= '({} {} {})'.format(content_string, excl_string,filter_string)

    # instantiate twitter api
    logging.info("Initializing Twitter API")
    getTwitter = GetTwitter()
    max_tweets = 100
    date_since = "2019-06-01"

    logging.info("Getting Tweets from Twitter API")
    tweet_id,tweet_text, tweet_location, tweet_time = getTwitter.getTweetsbyQuery(jw_query, max_tweets,date_since)
    print(json.dumps(getTwitter.api.rate_limit_status()['resources']['search']))

    books_just_read = pd.DataFrame([tweet_id,tweet_text, tweet_location, tweet_time]).transpose()
    books_just_read.columns = ['id','tweet','location','time']
    books_just_read.to_csv(".//Database//books_just_read.csv",encoding='utf-8-sig')


if __name__ == '__main__':
    getJustRead()