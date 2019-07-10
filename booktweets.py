import pandas as pd
import numpy as np
import re
import datetime
import logging
from twitter_api import GetTwitter
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)

book_titles_path = ".//Database//books_titles.csv"
book_tweets_path = ".//Database//books_tweets.csv"

class BookTweets():

    def main(self):
        self.loadTitles()
        self.getUniqueTitles()
        self.createQueries()
        self.getTweets()

    def loadTitles(self):
        self.book_titles = pd.read_csv(book_titles_path, index_col=0)
        return self.book_titles

    def getUniqueTitles(self):
        titles = np.unique(self.book_titles.title)
        return titles

    def createQueries(self):
        titles = self.getUniqueTitles()
        filter = "-filter:retweets -filter:links -filter:replies"
        content = "AND (read OR reads OR reading OR book OR books OR novel OR author)"

        queries = []
        for title in titles:
            title = re.sub("[\(\[].*?[\)\]]", "", title)
            title = title.rstrip()
            query = '"{}" OR "{}" {} {}'.format(title,title.lower(),content,filter)
            queries.append(query)

        queries_df = pd.DataFrame({'title':titles,'queryString':queries})
        return queries_df

    def getTweets(self, batch_size=100):

        queries_df = self.createQueries()
        tw = GetTwitter()

        max_tweets = 200
        date_since = '2019-06-01'
        rawTweets, tweetId, spacyTweets, vaderTweets, tweetTime, tweetLocation, titleTracker = \
        [],[],[],[],[],[],[]
        counter, tweets_counter = 0,0

        logging.info("Getting Tweets from Twitter API")
        for index, row in tqdm(queries_df.iterrows()):
            if counter < batch_size:
                try:
                    tweet_id, tweet_text, tweet_location, tweet_time = \
                        tw.getTweetsbyQuery(row['queryString'],max_tweets, date_since)
                    spacy_clean, vader_clean = tw.spacy_clean(tweet_text), tw.vader_clean(tweet_text)
                    tweetId.append(tweet_id)
                    rawTweets.append(tweet_text)
                    spacyTweets.append(spacy_clean)
                    vaderTweets.append(vader_clean)
                    tweetTime.append(tweet_time)
                    tweetLocation.append(tweet_location)
                    titleTracker.append(row['title'])
                    counter += 1
                    tweets_counter += len(tweet_text)
                    logging.info("Processing title {}".format(counter))
                    print(tw.api.rate_limit_status()['resources']['search'])
                    if counter % 150 == 0:
                        time.sleep(60)
                        logging.info("Pausing. Will resume in 5 seconds...")
                except Exception as ex:
                    print(ex)
                    continue

        logging.info("Received tweets for {} titles".format(counter))
        logging.info("{} tweets recieved from Twitter API".format(tweets_counter))

        self.book_tweets = pd.DataFrame({'tweetId': tweetId,
                                    'title': titleTracker,
                                    'tweetTime': tweetTime,
                                    'rawTweets': rawTweets,
                                    'spacyTweets': spacyTweets,
                                    'vaderTweets': vaderTweets,
                                    'location': tweetLocation
                                    })
        self.flatten_tweets()
        self.addQueryCounts()
        self.getEarliestTimeStamp()
        return self.book_tweets

    def flatten_tweets(self):
        rawTweets, tweetIds, spacyTweets, vaderTweets, tweetTimes, tweetLocations, titles, titleTracker = \
        [],[],[],[],[],[],[],[]

        for index, row in self.book_tweets.iterrows():
            for id, tw_time, raw, spacytw, vadertw, location in zip(row.tweetId, row.tweetTime, row.rawTweets,row.spacyTweets,row.vaderTweets, row.location):
                tweetIds.append(id)
                tweetTimes.append(tw_time)
                rawTweets.append(raw)
                spacyTweets.append(spacytw)
                vaderTweets.append(vadertw)
                tweetLocations.append(location)
                titles.append(row.title)

        flat_df = pd.DataFrame({'tweetId':tweetIds,'tweetTime':tweetTimes,'title':titles,'rawTweet':rawTweets,'spacyTweet':spacyTweets,'vaderTweet':vaderTweets,'location':tweetLocations})
        flat_df.reset_index(drop=True)
        self.book_tweets = flat_df

    def addQueryCounts(self):
        logging.info("Adding query counts to book titles")
        tweets_df = self.book_tweets
        books_df = self.book_titles

        currTime = datetime.datetime.utcnow()
        # Collecting counts for past 7 day of query results
        deadline7 = currTime + datetime.timedelta(-7)
        indexNames7 = tweets_df[tweets_df['tweetTime'] < deadline7].index
        tweets_df7 = tweets_df.drop(indexNames7, inplace=False)
        latest_df7 = tweets_df7.reset_index(drop=True)
        counts_df7 = latest_df7['title'].value_counts().rename_axis('title').reset_index(name='7day')

        books_df = books_df.merge(counts_df7,how='left',on='title')

        logging.info("Query counts successfully added")

    def getEarliestTimeStamp(self):
        logging.info("Add earliest tweet time stamp")
        tweets_df = self.book_tweets
        books_df = self.book_titles

        for index, row in books_df.iterrows():
            try:
                earliest_tweet = min(tweets_df.loc[tweets_df['title'] == row.title]['tweetTime'])
            except:
                earliest_tweet = None
            books_df.at[index,'earliestTweet'] = earliest_tweet
        self.book_titles = books_df

    def writeToDb(self):
        self.book_titles.to_csv(book_titles_path, encoding='utf-8-sig')
        self.book_tweets.to_csv(book_tweets_path,encoding='utf-8-sig')
        logging.info("Database updated")

if __name__ == '__main__':
    book_tw = BookTweets()
    book_tw.main()
    book_tw.writeToDb()











