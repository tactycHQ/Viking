import sys
import requests
from tqdm import tqdm
from xml.etree import ElementTree
from config import get_config_from_json
import time
import json
from bs4 import BeautifulSoup
import html5lib
import re
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.INFO)

GRPopularURL = "https://www.goodreads.com/book/popular_by_date/2019/5?id=2019/May"
GBpath = """https://www.googleapis.com/books/v1/volumes?q="{}"""""
popular_path = ".//Database//goodreads_popular.csv"
book_titles_path = ".//Database//books_titles.csv"

class Books():

    def __init__(self):
        keys = get_config_from_json('.//Keys//keys.json')
        self.gr_key = keys['goodreads_key']['api_key']

    def main(self,maxtitles=200):
        self.maxtitles = maxtitles
        try:
            self.book_titles = pd.read_csv(book_titles_path,index_col=0)
            logging.info("Found existing book_titles. Appending to it")
            self.updateDB()
        except IOError as e:
            logging.info("No existing book_titles found")
            self.createNewDB()
        except Exception as ex:
            print(ex)
            sys.exit(1)

        self.writeToDB()


    def getGRpopular(self):
        logging.info("Getting latest copy of popular titles from Goodreads...")

        r = requests.get(GRPopularURL)
        soup = BeautifulSoup(r.content,'html5lib')
        titles = soup.findAll("a", attrs={'class':'bookTitle'})
        title_list = []

        for count, title in tqdm(enumerate(titles)):
            if count == self.maxtitles:
                break
            else:
                t = title.text.strip('\n').lstrip().rstrip()
                title_list.append(t)
        logging.info("Received data on popular books from Goodreads for {} titles".format(len(title_list)))

        goodreads_popular = pd.DataFrame(title_list, columns=['title'])
        goodreads_popular.index.name = "popId"
        goodreads_popular.to_csv(popular_path)
        return goodreads_popular

    def getGRmeta(self, title):
        show_URL = "https://www.goodreads.com/book/title.xml?key={}&title={}".format(self.gr_key,title)

        r = requests.get(show_URL)
        resp = r.content
        tree = ElementTree.ElementTree(ElementTree.fromstring(resp))
        root = tree.getroot()

        gr_meta ={}
        gr_meta["title"] = title

        for child in root.iter('isbn'):
            gr_meta["isbn"] = child.text
            break

        for child in root.iter('isbn13'):
            gr_meta["isbn13"] = child.text
            break

        for child in root.iter('name'):
            gr_meta["author"] = child.text
            break

        for child in root.iter('description'):
            gr_meta["description"] = child.text
            break

        for child in root.iter('ratings_count'):
            gr_meta["ratings_count"] = child.text
            break

        for child in root.iter('average_rating'):
            gr_meta["average_rating"] = child.text
            break

        for child in root.iter('publisher'):
            gr_meta["publisher"] = child.text
            break

        for child in root.iter('publication_year'):
            gr_meta["publication_year"] = child.text
            break

        for child in root.iter('publication_month'):
            gr_meta["publication_month"] = child.text
            break

        for child in root.iter('publication_day'):
            gr_meta["publication_day"] = child.text
            break

        genres = {}
        for child in root.iter('shelf'):
            genres[child.attrib['name']] = child.attrib['count']
        gr_meta["genres"] = genres

        return gr_meta

    def getGB_metadata(self, title):

        title_path = GBpath.format(title)
        response = requests.get(title_path)
        response_json = json.loads(response.text)

        title_name = []
        author=[]
        publisher=[]
        publishedDate=[]
        description=[]
        isbn_13=[]
        isbn_10=[]
        categories = []
        gb_rating = []
        gb_ratingcount = []

        title_name.append(title)
        try:
            author.append(response_json['items'][0]['volumeInfo']['authors'])
        except:
            author.append(np.nan)
        try:
            publisher.append(response_json['items'][0]['volumeInfo']['publisher'])
        except:
            publisher.append(np.nan)
        try:
            publishedDate.append(response_json['items'][0]['volumeInfo']['publishedDate'])
        except:
            publishedDate.append(np.nan)
        try:
            description.append(response_json['items'][0]['volumeInfo']['description'])
        except:
            description.append(np.nan)
        try:
            isbn_13.append(response_json['items'][0]['volumeInfo']['industryIdentifiers'][0]['identifier'])
        except:
            isbn_13.append(np.nan)
        try:
            isbn_10.append(response_json['items'][0]['volumeInfo']['industryIdentifiers'][1]['identifier'])
        except:
            isbn_10.append(np.nan)
        try:
            categories.append(response_json['items'][0]['volumeInfo']['categories'])
        except:
            categories.append(np.nan)
        try:
            gb_rating.append(response_json['items'][0]['volumeInfo']['averageRating'])
        except:
            gb_rating.append(np.nan)
        try:
            gb_ratingcount.append(response_json['items'][0]['volumeInfo']['ratingsCount'])
        except:
            gb_ratingcount.append(np.nan)

        title_name, author, publisher, publishedDate, description, isbn_13, isbn_10, categories, gb_rating, gb_ratingcount = \
        (np.array(title_name[0]),
        np.array(author[0]),
         np.array(publisher[0]),
         np.array(publishedDate[0]),
         np.array(description[0]),
         np.array(isbn_13[0]),
         np.array(isbn_10[0]),
         np.array(categories[0]),
         np.array(gb_rating[0]),
         np.array(gb_ratingcount[0]))

        print(title_name,author, publisher, publishedDate, description, isbn_13, isbn_10, categories, gb_rating, gb_ratingcount)
        return title_name,author, publisher, publishedDate, description, isbn_13, isbn_10, categories, gb_rating, gb_ratingcount

    def createNewDB(self):
        logging.info("Creating new books database")

        popdf = self.getGRpopular()
        # popdf = pd.read_csv(popular_path)
        pop_titles = popdf['title'].values
        master_books = pd.DataFrame()

        counter=0
        for title in tqdm(pop_titles):
            gr_meta = self.getGRmeta(title)
            master_books = master_books.append(gr_meta, ignore_index=True)
            counter+=1
            time.sleep(2)

        logging.info("Created new books data with {} titles".format(counter))
        self.book_titles = master_books


    def updateDB(self):

        popdf = self.getGRpopular()
        # popdf = pd.read_csv(popular_path)
        curr_titles = self.book_titles['title'].values
        unique_titles = [title for title in popdf['title'].values if title not in curr_titles]

        logging.info("Getting metadata for {} new titles".format(len(unique_titles)))

        counter=0
        for title in tqdm(unique_titles):
            gr_meta = self.getGRmeta(title)
            self.book_titles = self.book_titles.append(gr_meta, ignore_index=True)
            counter += 1
            time.sleep(2)

        logging.info("{} new titles added to database".format(counter))

    def writeToDB(self):
        self.book_titles.to_csv(book_titles_path)

if __name__ == '__main__':

    books = Books()
    books.main(maxtitles = 8)
