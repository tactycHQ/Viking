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

GRPopularURL = "https://www.goodreads.com/book/popular_by_date/2019/6?id=2019/June"
GBpath = """https://www.googleapis.com/books/v1/volumes?q="{}"""""
popular_path = ".//Database//goodreads_popular.csv"
master_book_path = ".//Database//books.csv"

class Books():

    def __init__(self):
        keys = get_config_from_json('.//Keys//keys.json')
        self.gr_key = keys['goodreads_key']['api_key']

    def getGRpopular(self):
        r = requests.get(GRPopularURL)

        soup = BeautifulSoup(r.content,'html5lib')

        titles = soup.findAll("a", attrs={'class':'bookTitle'})
        title_list = []
        for title in tqdm(titles):
            # t = re.sub("[\(\[].*?[\)\]]", "", title.text)
            t = title.text.strip('\n').lstrip().rstrip()
            title_list.append(t)

        logging.info("Received data on popular books from Goodreads")

        goodreads_popular = pd.DataFrame(title_list, columns=['title'])
        goodreads_popular.index.name = "popId"
        goodreads_popular.to_csv(popular_path)

        return title_list

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

        popdf = pd.read_csv(popular_path)
        pop_titles = popdf['title'].values
        master_books = pd.DataFrame()

        counter = 0
        for title in tqdm(pop_titles):
            gr_meta = self.getGRmeta(title)
            master_books = master_books.append(gr_meta, ignore_index=True)
            counter+=1
            time.sleep(2)

        logging.info("Created new books data with {} titles".format(counter))
        self.writeToDB(master_books,master_book_path)
        return master_books

    def writeToDB(self, df, path):
        df.to_csv(path)
        logging.info("Database updated")

    def updateDB(self):

        try:
            books = pd.read_csv(".//Database//books.csv")
            logging.info("Found books database. Appending to current database")

            # pop_titles = getGRPopular()
            pop_titles = self.getGRpopular()
            curr_titles = books['title'].values

            counter = 0
            for title in pop_titles:
                if title in curr_titles:
                    logging.info("{} is ignored as it is a duplicate".format(title))
                else:
                    counter += 1
                    if counter % 4 == 0:
                        logging.info("Resetting...")
                        time.sleep(5)
                    else:
                        title_name, author, publisher, publishedDate, description, isbn_13, isbn_10, categories, gb_rating, gb_ratingcount = \
                            self.getGB_metadata(title)
                        new_row = pd.DataFrame(
                            [(title_name, author, publisher, publishedDate, description, isbn_13, isbn_10, categories,
                              gb_rating, gb_ratingcount)],
                            columns=['title', 'author', 'publisher', 'publishedDate', 'description', 'isbn_13',
                                     'isbn_10', 'categories', 'gb_rating', 'gb_ratingcount'])
                        books = pd.concat([books, new_row], axis=0, sort=False, ignore_index=True)
                        logging.info("{} added to books database".format(title))



            books.set_index(['id'],inplace=True)
            books.reset_index(inplace=True, drop=True)
            books.to_csv(".//Database//books.csv")

        except Exception as ex:
            print(ex)
            logging.info("No existing database found. Creating new...")
            pop_titles = "Moonwalking with Einstein"
            title_name, author, publisher, publishedDate, description, isbn_13, isbn_10, categories, gb_rating, gb_ratingcount = \
                self.getGB_metadata(pop_titles)
            books = pd.DataFrame([(title_name,author,publisher, publishedDate,description, isbn_13,isbn_10,categories,gb_rating,gb_ratingcount)],
                         columns=['title','author','publisher', 'publishedDate','description', 'isbn_13','isbn_10','categories','gb_rating','gb_ratingcount'])
            books.index.name = "id"
            books.to_csv(".//Database//books.csv")

if __name__ == '__main__':

    books = Books()
    # books.getGRpopular()
    # print(books.getGRmeta("Julie of the Wolves (Julie of the Wolves, #1)"))
    books.createNewDB()
    # print(books.getGRtitle("Julie of the Wolves (Julie of the Wolves, #1)"))
    # print(books.getGB_metadata())
    # books.updateDB()