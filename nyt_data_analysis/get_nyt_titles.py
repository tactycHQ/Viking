import json
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
import requests
import time

key = 'tCISfaYAG5YwMeALKFoOzUyJrcFuhrew'

def getDates():
    dates=[]
    for i in range (0,156):
        start_date = '2019-07-01'
        datetime_object = datetime.strptime(start_date, '%Y-%m-%d')
        prior_date = datetime_object - timedelta(days=7*i)
        datetime_string = datetime.strftime(prior_date, '%Y-%m-%d')
        dates.append(datetime_string)
    return dates

def getBestSellers(dates):

    count=0
    nyt_df = pd.DataFrame()

    for date in tqdm(dates):
        print("Processing: {}".format(date))
        time.sleep(6.5)
        url= 'https://api.nytimes.com/svc/books/v3/lists/{}/combined-print-and-e-book-nonfiction.json?api-key={}'.format(date,key)

        try:
            r = requests.get(url)
            results = r.json()
            books = results['results']['books']
            best_date = results['results']['bestsellers_date']
            pub_date = results['results']['published_date']

            title =[]
            isbn10 = []
            isbn13 = []
            publisher = []
            author = []
            weeks_on_list = []
            bestsellers_date = []
            published_date = []
            tempdf = pd.DataFrame()
            for book in books:
                title.append(book['title'])
                isbn10.append(book['isbns'][0]['isbn10'])
                isbn13.append(book['isbns'][0]['isbn13'])
                publisher.append(book['publisher'])
                author.append(book['author'])
                weeks_on_list.append(book['weeks_on_list'])
                bestsellers_date.append(best_date)
                published_date.append(pub_date)
                tempdf = pd.DataFrame(list(zip(title, isbn10, isbn13, publisher, author, weeks_on_list, bestsellers_date, published_date)),
                                  columns=["title", "isbn10", "isbn13", "publisher", "author", "weeks_on_list",
                                           "bestsellers_date","published_date"], )
            nyt_df = nyt_df.append(tempdf,ignore_index=True)
            count += 1
            print(count)

        except Exception as ex:
            print(ex)

    nyt_df.to_csv(("..//Database//nyt_df.csv"))
    return nyt_df

if __name__ == '__main__':
    getBestSellers(getDates())
