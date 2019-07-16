from   selenium                           import webdriver
from   selenium.webdriver.firefox.options import Options
from   selenium.webdriver.common.keys     import Keys
from   bs4                                import BeautifulSoup
import pandas as pd
import time
import logging
logging.basicConfig(level=logging.INFO)

month = 'may'
year = '2016'
EMAIL    = '############'
PASSWORD = '############'
LINK1     = 'https://www.goodreads.com/'
save_path = '..//Database/MonthlyReleaseIDs/{}{}.csv'.format(month,year)

def load_access():
    with open('access.txt','r') as ac:
        data       = ac.read()
        data_parts = data.split('\n')
        EMAIL      = data_parts[0].strip()
        PASSWORD   = data_parts[1].strip()

    return EMAIL,PASSWORD

def create_driver():
    options = webdriver.ChromeOptions()
    options.binary_location = 'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe'
    options.add_argument('--headless')
    driver = webdriver.Chrome(executable_path='C:/Users/anubhav/Desktop/Projects/Viking/chromedriver.exe',
                              options=options)
    driver.get(LINK1)
    soup = BeautifulSoup(driver.page_source, 'lxml')

    EMAIL, PASSWORD = load_access()
    username = driver.find_element_by_id("userSignInFormEmail").send_keys(EMAIL)
    password = driver.find_element_by_id("user_password")
    password.send_keys(PASSWORD)
    password.submit()
    return driver


def get_my_books_page(driver):

    ids = []
    count = 0

    for i in range(1, 25):
        logging.info("Processing page {}".format(i))
        LINK2 = 'https://www.goodreads.com/shelf/show/{}-{}?page={}'.format(month,year,i)
        driver.get(LINK2)

        soup = BeautifulSoup(driver.page_source, 'html5lib')
        input_tags = soup.findAll("input",  {"id": "book_id", 'type': "hidden"})

        for tag in input_tags:
            if tag.has_attr('value'):
                if tag['value'] not in ids:
                    ids.append(tag['value'])
                    count += 1
                    logging.info("Added title {}".format(count))
        time.sleep(5.0)

    logging.info("Received data on popular books from Goodreads for {} titles".format(len(ids)))
    grmonth = pd.DataFrame(ids, columns=['gr_id'])
    grmonth.index.name = "id"
    grmonth.to_csv(save_path)
    driver.quit()

if __name__ == '__main__':
    driver = create_driver()
    get_my_books_page(driver)


