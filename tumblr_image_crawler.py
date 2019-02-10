# coding=utf-8:
import requests
from time import time
import urllib.request
import asyncio
from bs4 import BeautifulSoup
import datetime
import re
import os
import json
import mysql.connector
from mysql.connector import Error

FILE_LOCATED_PATH = os.path.dirname(os.path.abspath(__file__))
IMAGES_VAULT_DIRECTORY_NAME = FILE_LOCATED_PATH + "/raw_vault"
IMAGES_DIRECTORY_NAME = FILE_LOCATED_PATH + "/images"
LOGS_DIRECTORY_NAME = FILE_LOCATED_PATH + "/logs"
CONFIG_DIRECTORY_NAME = FILE_LOCATED_PATH + "/config"

required_dirs = [IMAGES_VAULT_DIRECTORY_NAME, IMAGES_DIRECTORY_NAME, LOGS_DIRECTORY_NAME, CONFIG_DIRECTORY_NAME]

def init_dirs():
    for dir in required_dirs:
        make_dir(dir)

def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def gex_max_article_id():
    try:
        id_file_r = open(CONFIG_DIRECTORY_NAME+"/max_id.txt", "r")
        max_id = id_file_r.read()
        id_file_r.close()
        return max_id
    except FileNotFoundError:
        print("id information file does not exist. set to default value(0)")
        return 0


def set_max_article_id(max_article_id):
    id_file_w = open(CONFIG_DIRECTORY_NAME+"/max_id.txt", "w+")
    id_file_w.write(str(max_article_id))
    id_file_w.close()

def get_html(url):
    html = ""
    resp = requests.get(url)
    if resp.status_code == 200:
        html = resp.text
    return html

def get_image_urls(html_text):
    soup = BeautifulSoup(html_text, 'html.parser')
    nick = soup.find("strong", {"class" : "nick"}).text
    srl = soup.find("span", {"class" : "member_srl"}).text.replace("(", "").replace(")","")
    images = soup.find("div", {"class" : "board_main_view"}).find_all("img")
    img_infos = []
    for image in images:
        if(image["src"].startswith("//")):
            img_infos.append({"nick" : nick, "srl": srl, "url" : image["src"].replace("//", "https://")})
        else:
            img_infos.append({"nick" : nick, "srl": srl, "url" : image["src"]})

    return img_infos

def prepare_urls():
    # 각종 디렉토리 세팅.
    init_dirs()
    # 이전 작업에서 저장된 마지막 게시물 ID를 불러옴.
    max_id = gex_max_article_id()

    urls = []
    URL = "http://bbs.ruliweb.com/community/board/300143"
    html = get_html(URL)
    soup = BeautifulSoup(html, 'html.parser')

    list_table = soup.find("table", {"class" : "board_list_table"}) #아티클 테이블

    id_tds = list_table.find_all("td", {"class" : "id"}) #id 정보 리스트
    article_ids = list(map(lambda a: int(a.text.replace("\n","").replace(" ","")), id_tds))
    max_article_id = max(article_ids)

    # 이번 조회에서 얻은 마지막 게시물 ID를 저장함.
    set_max_article_id(max_article_id)

    before_filter_articles = list_table.find_all("tr", {"class" : "table_body"})
    after_filter_articles = []
    for article in before_filter_articles:
        id = article.find("td", {"class" : "id"}).text.replace("\n","").replace(" ","")
        picture_ico = article.find("i",{"class" : "icon-picture"})
        #id 가 크면서 이미지가 존재하는 게시물인 경우.. 수집 항목에 추가.
        if (int(id) > int(max_id) and picture_ico != None):
            after_filter_articles.append(article)

    for article in after_filter_articles:
        relative = article.find_all("div", {"class" : "relative"})
        anchor = article.find("a", {"class" : "deco"})
        urls.append(anchor["href"])
    return urls


def download_img(url_info):
    try:
        url = url_info["url"]
        personal_dir_name = url_info["nick"] + "(" + url_info["srl"] + ")"
        file_name = url.split("/").pop()
        if (len(file_name.split(".")) < 2): #확장자가 없는 이미지는 다운로드하지 않음.
            print(url," : no file extension. skip download")
            return 0

        save_path = IMAGES_DIRECTORY_NAME + "/" + datetime.datetime.now().strftime("%Y-%m-%d") + "/" + personal_dir_name

        try:
            make_dir(save_path)
        except Exception as ex:
            print("directory creation error(maybe it already exists.), continue download.. ", ex)
            
        save_fullpath = save_path + "/" + datetime.datetime.now().strftime('%Y%m%d%H%M%S%f') +"." + file_name.split(".").pop()
        urllib.request.urlretrieve(url, save_fullpath)
        size = int(os.path.getsize(save_fullpath)/1024)
        return (url_info["srl"], url_info["nick"], url, save_fullpath, size)
    except Exception as ex:
        print(url + " Failed to download image..", ex)
        return 0

async def fetch(url):
    response = await loop.run_in_executor(None, get_html, url)    # run_in_executor 사용
    img_srcs = await loop.run_in_executor(None, get_image_urls, response)   # run in executor 사용
    return img_srcs

async def find_main(urls):
    futures = [asyncio.ensure_future(fetch(url)) for url in urls]   # 태스크(퓨처) 객체를 리스트로 만듦
    result = await asyncio.gather(*futures)                # 결과를 한꺼번에 가져옴
    return result

async def download_fetch(url_info):
    response = await loop.run_in_executor(None, download_img, url_info)    # run_in_executor 사용
    return response

async def download_main(url_infos):
    futures = [asyncio.ensure_future(download_fetch(url_info)) for url_info in url_infos]   # 태스크(퓨처) 객체를 리스트로 만듦
    result = await asyncio.gather(*futures)                # 결과를 한꺼번에 가져옴
    return result


# get database connection

with open("./config/database.json", "r") as f:
    database_info = json.load(f)

host = database_info["host"]
database = database_info["database"]
user= database_info["user"]
password = database_info["password"]

try:
    connection = mysql.connector.connect(host=host, database=database, user=user, password=password)
    if (connection.is_connected()):

        #create cursor..
        cursor = connection.cursor(prepared=True)

        find_begin = time()
        urls = prepare_urls()
        loop = asyncio.get_event_loop() # 이벤트 루프를 얻음
        image_urls = loop.run_until_complete(find_main(urls))   # main이 끝날 때까지 기다림
        find_end = time()
        download_begin = time()

        found_image_count = 0
        if (len(image_urls)) > 0 :
            flatten_image_url_infos = [item for sublist in image_urls for item in sublist]
            found_image_count = len(flatten_image_url_infos)
            
            user_add_query = """INSERT INTO ruliweb_users (user_nick, user_srl) SELECT * FROM (SELECT %s as v1, %s as v2) AS tmp WHERE NOT EXISTS (SELECT user_srl FROM ruliweb_users WHERE user_srl = %s) LIMIT 1"""

            user_list = list(map(lambda x:(x["nick"],x["srl"],x["srl"]), flatten_image_url_infos))
            uniq_user_list = list(set(user_list))           
            result  = cursor.executemany(user_add_query, uniq_user_list)
            connection.commit()
            print (cursor.rowcount, " new users inserted successfully into users table")

            downloaded_file_infos = loop.run_until_complete(download_main(flatten_image_url_infos))  # main이 끝날 때까지 기다림
            loop.close()

            image_info_add_query = """INSERT INTO ruliweb_image_infos (user_srl, user_nickname, image_path, image_local_path, size, created_at) VALUES (%s,%s,%s,%s,%s,NOW()) """
            download_success_file_list = list(filter(lambda x:x != 0, downloaded_file_infos))
            result2 = cursor.executemany(image_info_add_query, download_success_file_list)
            connection.commit()
            print( cursor.rowcount, " new image inserted successfully into ruliweb_image_infos table")
            
            update_user_list = list(map(lambda x:(x[1],x[1]), uniq_user_list))
            user_image_count_update_query = """update ruliweb_users set image_count = (SELECT count(*) from ruliweb_image_infos where user_srl = %s) where user_srl = %s """
            result3 = cursor.executemany(user_image_count_update_query, update_user_list)
            connection.commit()
            print( cursor.rowcount, " user`s image count data updated..")

            #loop.close()
        else:
            print('no image.')
            loop.close()

        download_end = time()

        if (found_image_count > 0) :
            print('==============================================================')
            print (datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
            print(' %d images found.'%(found_image_count))
            print('Searching time : {0:.3f} second'.format(find_end - find_begin))
            print('Download time : {0:.3f} second'.format(download_end - download_begin))
            print('==============================================================')

except mysql.connector.Error as error:
    print("Failed to connect DB, error :  {}".format(error))

finally:
    if(connection.is_connected()):
        cursor.close()
        connection.close()
        print("DB Connection closed sucessfully.")

