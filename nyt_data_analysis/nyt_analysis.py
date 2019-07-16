import pandas as pd
import os
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import glob
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np
import twint

def combinemonthlies():
    os.chdir("../Database/MonthlyReleaseIDs")
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    df = pd.concat([pd.read_csv(f) for f in all_filenames])

    print(df.shape)
    df.drop_duplicates(subset=['gr_id'],keep='last',inplace=True)
    print(df.shape)

    dropIndex = df[df.id>400].index
    df.drop(index=dropIndex,inplace=True)
    print(df.shape)
    eta = df.shape[0]*2/3600
    print(eta)

    df = df.drop(['id'],axis=1)
    df.reset_index(drop=True, inplace=True)
    df.index.name='id'
    df.to_csv( "combined.csv", index=False, encoding='utf-8-sig')

def addnytflags():
    nyt_df = pd.read_csv('../Database/nyt_df_week1_clean.csv',index_col=0)
    combined_df = pd.read_csv('../Database/combined_titles_clean.csv',index_col=0)
    nyt_df.isbn10.apply(str)
    nyt_df.clean_title = nyt_df.clean_title.str.strip()
    combined_df.isbn.apply(str)
    combined_df.clean_title = combined_df.clean_title.str.strip()


    for index, row in combined_df.iterrows():
        if row.publication_year == 2017 and row.publication_month>=6 and row.publication_day>0:
            combined_df.at[index,'publication_date'] = datetime(year=int(row.publication_year),month=int(row.publication_month),day=int(row.publication_day))
        elif row.publication_year > 2017 and row.publication_month > 0 and row.publication_day >0:
            combined_df.at[index,'publication_date'] = datetime(year=int(row.publication_year),month=int(row.publication_month),day=int(row.publication_day))

    combined_df = combined_df[combined_df.publication_date>=datetime(year=2017,month=6,day=1)]

    count=0
    for index,row in nyt_df.iterrows():
        i = combined_df[(combined_df.clean_title == row.clean_title) & (combined_df.author == row.author)].index
        if i.size>0:
            count+=1
            combined_df.at[i.values,'nyt'] = 1
            combined_df.at[i.values, 'nyt_best_date'] = row.bestsellers_date
            combined_df.at[i.values, 'nyt_pub_date'] = row.published_date

    print('{} NYT titles matched'.format(count))
    print(combined_df.head(3))
    combined_df.to_csv('../Database/combined_titles_nyt.csv',encoding='utf-8-sig')

def Queries():
    df = pd.read_csv('../Database/combined_titles_nyt.csv', index_col=0,encoding='utf-8-sig')
    for index,row in df.iterrows():
        df.at[index,'since'] = row.publication_date
        df.at[index, 'until'] = datetime.strptime(row.publication_date,'%Y-%m-%d') + timedelta(days=15)

    for index,row in df.iterrows():
        query = '{} AND {} -filter:retweets -filter:links -filter:replies since:{} until:{}'.format(row.clean_title,row.author,row.since,row.until)
        query = query[:-9]
        df.at[index,'queries'] = query

    print(df.head(3))
    df.to_csv('../Database/combined_titles_nyt_queries.csv')

def QueriesToday():
    df = pd.read_csv('../Database/combined_titles_nyt.csv', index_col=0,encoding='utf-8-sig')
    for index,row in df.iterrows():
        df.at[index,'since'] = '2019-07-01'
        df.at[index, 'until'] = '2019-07-15         '

    for index,row in df.iterrows():
        query = '{} AND {} -filter:retweets -filter:links -filter:replies since:{} until:{}'.format(row.clean_title,row.author,row.since,row.until)
        query = query[:-9]
        df.at[index,'queries'] = query

    print(df.head(3))
    df.to_csv('../Database/combined_titles_nyt_queries.csv',encoding='utf-8-sig')

def getTweets():
    df = pd.read_csv('../Database/combined_titles_nyt_queries.csv', index_col=0, encoding='utf-8-sig')
    tweets_df=pd.DataFrame(columns=['title','tweet'])

    for index,row in df.iterrows():
        tweets = []
        print("-----{}------".format(row.title))
        c = twint.Config()
        # c.Format = "Tweet: {tweet} | Date:{date}"
        c.Limit = 200
        c.Store_object = True
        c.Store_object_tweets_list = tweets
        c.Search = row.queries
        twint.run.Search(c)
        for tw in tweets:
            dict = {'title':row.title,'tweet':tw.tweet}
            tweets_df = tweets_df.append(dict,ignore_index=True)
            df.at[index,'tweet_count'] = len(tweets)
    df.to_csv('../Database/combined_titles_nyt_tweets.csv')
    tweets_df.to_csv('../Database/tweets.csv')

if __name__ == '__main__':
    # addnytflags()
    # Queries()
    # QueriesToday()
    getTweets()