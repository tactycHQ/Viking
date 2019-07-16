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
releases_path = ".//Database//combined.csv"
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
        input_tags = soup.findAll("input",{"id":"book_id"})

        ids = []
        count = 0
        for tag in input_tags:
            if count == self.maxtitles:
                break
            else:
                if tag["value"] is not None and tag["value"] not in ids:
                    ids.append(tag["value"])
                    count += 1

        logging.info("Received data on popular books from Goodreads for {} titles".format(len(ids)))
        goodreads_popular = pd.DataFrame(ids, columns=['gr_id'])
        goodreads_popular.index.name = "id"
        goodreads_popular.to_csv(popular_path)
        return goodreads_popular

    def getGRmeta(self, id):
        show_URL = "https://www.goodreads.com/book/show/{}.xml?key={}".format(id,self.gr_key)
        # show_URL = "https://www.goodreads.com/book/show/{}.xml".format(id)

        r = requests.get(show_URL)
        resp = r.content
        tree = ElementTree.ElementTree(ElementTree.fromstring(resp))
        # tree = ElementTree.ElementTree(ElementTree.fromstring(resp,ElementTree.XMLParser(encoding='utf-8')))
        root = tree.getroot()

        gr_meta ={}

        gr_meta["gr_id"] = id

        for child in root.iter('title'):
            gr_meta["title"] = child.text
            break

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

        # popdf = self.getGRpopular()
        popdf = pd.read_csv(releases_path)
        pop_id = popdf['gr_id'].values
        master_books = pd.DataFrame()

        counter=0
        for id in tqdm(pop_id):
            gr_meta = self.getGRmeta(id)
            master_books = master_books.append(gr_meta, ignore_index=True)
            counter+=1
            time.sleep(1.5)
            if counter % 50 == 0:
                master_books.to_csv(book_titles_path)

        logging.info("Created new books data with {} titles".format(counter))
        self.book_titles = master_books


    def updateDB(self):

        popdf = self.getGRpopular()
        # popdf = pd.read_csv(releases_path)
        curr_id = self.book_titles['gr_id'].astype(int).tolist()
        unique_ids = [id for id in popdf['gr_id'].astype(int).tolist() if id not in curr_id]

        logging.info("Getting metadata for {} new titles".format(len(unique_ids)))

        counter=0
        for id in tqdm(unique_ids):
            gr_meta = self.getGRmeta(id)
            self.book_titles = self.book_titles.append(gr_meta, ignore_index=True)
            counter += 1
            time.sleep(1.5)

        logging.info("{} new titles added to database".format(counter))

    def writeToDB(self):
        self.book_titles.to_csv(book_titles_path)

if __name__ == '__main__':

    books = Books()
    # books.getGRpopular()
    # books.main(maxtitles = 200)
    books.createNewDB()
    books.writeToDB()