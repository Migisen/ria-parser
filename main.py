from multiprocessing import Pool
from time import sleep

import dateparser
import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
from fake_useragent import UserAgent

from models import init_db, session_scope, RiaNews, engine


def riaParser(url, loops=1, timeout=2):
    while loops != 0:
        ria = requests.get(url, headers={'User-Agent': UserAgent().chrome})
        processes = []
        if ria.status_code == 200:
            result = []
            ria_soup = bs(ria.content, features="html.parser")
            # Получаем следующую ссылку
            try:
                url = "https://ria.ru" + ria_soup.find(class_="list-items-loaded").get("data-next-url")
            except AttributeError:
                url = "https://ria.ru" + ria_soup.find(class_="list-more").get("data-url")
            news_block = ria_soup.findAll(class_="list-item")
            result = news_scraper(news_block)

            with session_scope() as session:
                for first in result:
                    if first['Text'] is not None:
                        instance = RiaNews(
                            title=first['Title'],
                            date=first['Date'],
                            url=first['URL'],
                            url_next=url,
                            tags=first['Tags'],
                            text=first['Text']
                    )
                    session.add(instance)
            loops = loops - 1
    return


def news_scraper(news_block):
    result = []
    urls = []
    for news in news_block:
        if len(news.contents) == 3:
            # Основные сведения о новости
            article_time = news.find(class_="list-item__date")
            article_time = dateparser.parse(article_time.text, languages=['ru'])
            article_content = news.find(class_="list-item__content")
            article_url = article_content.contents[0].get("href")
            urls.append(article_url)
            article_tags = news.find(class_="list-item__tags").contents[1]
            tag_list = []
            for tag in article_tags:
                tag_list.append(tag.text)

            # Текст статьи
            # article_request = requests.get(article_url, headers={'User-Agent': UserAgent().chrome})
            # article_soup = bs(article_request.content, features="html.parser")
            # result_text = article_soup.findAll(class_="article__text")
            # article_text = ""
            # for paragraph in result_text:
            #    article_text = article_text + paragraph.text
            article = {"Title": article_content.text, "Date": article_time, "URL": article_url, "Tags": tag_list}
            result.append(article)
        else:
            pass
    pool = Pool(6)
    text_list = pool.map(get_text, urls)
    pool.close()
    pool.join()
    for first, second in zip(result, text_list):
        first["Text"] = second
    #print(result)
    return result


def get_text(url):
    try:
        article_request = requests.get(url, headers={'User-Agent': UserAgent().chrome})
        while article_request.status_code != 200:
            sleep(0.5)
            article_request = requests.get(url, headers={'User-Agent': UserAgent().chrome})
        article_soup = bs(article_request.content, features="html.parser")
        result_text = article_soup.findAll(class_="article__text")
        article_text = ""
        for paragraph in result_text:
            article_text = article_text + paragraph.text
        return article_text
    except Exception as e:
        print(e)


if __name__ == '__main__':
    init_db()
    #ria_url = "https://ria.ru/services/economy/more.html?id=1512761593&date=20180117T114843"
    #test =
    #while True:
    #    with session_scope() as session:
    #        ria_url = session.query(RiaNews).order_by(-RiaNews.id).first().url_next
    #    try:
    #        riaParser(ria_url, loops=1000)
    #    except Exception as e:
    #        print(e)
    with session_scope() as session:
        db = pd.read_sql_query(session.query(RiaNews).all(), con=engine)
