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

FILE_LOCATED_PATH = os.path.dirname(os.path.abspath(__file__))
# IMAGES_VAULT_DIRECTORY_NAME = FILE_LOCATED_PATH + "/raw_vault"
IMAGES_DIRECTORY_NAME = FILE_LOCATED_PATH + "/images"
# LOGS_DIRECTORY_NAME = FILE_LOCATED_PATH + "/logs"
# CONFIG_DIRECTORY_NAME = FILE_LOCATED_PATH + "/config"

required_dirs = [IMAGES_DIRECTORY_NAME]

def init_dirs():
    for dir in required_dirs:
        make_dir(dir)

def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def get_html(url):
    html = ""
    resp = requests.get(url)
    if resp.status_code == 200:
        html = resp.text
    return html

def get_image_urls(html_text):
    soup = BeautifulSoup(html_text, 'html.parser')
    title = soup.find("h2", {"class": "title"}).text
    section = soup.find("section", {"class" : "post"})
    body_text = section.find("div", {"class" : "body-text"})
    images = body_text.find_all("img")
    img_infos = []
    for image in images:
        try:
            origin_image = image["data-orig-src"]
            img_infos.append(origin_image)
        except Exception as ex:
            origin_image = image["src"]
            img_infos.append(origin_image)
    return img_infos, title


def download_img(url, title):
    try:
        file_name = url.split("/").pop()
        if (len(file_name.split(".")) < 2): #확장자가 없는 이미지는 다운로드하지 않음.
            print(url," : no file extension. skip download")
            return 0

        save_path = IMAGES_DIRECTORY_NAME + "/" + title

        try:
            make_dir(save_path)
        except Exception as ex:
            print("directory creation error(maybe it already exists.), continue download.. ", ex)
            
        save_fullpath = save_path + "/" + file_name
        urllib.request.urlretrieve(url, save_fullpath)
        size = int(os.path.getsize(save_fullpath)/1024)
        return (url, save_fullpath, size)
    except Exception as ex:
        print(url + " Failed to download image..", ex)
        return 0

async def download_fetch(url_info, title):
    response = await loop.run_in_executor(None, download_img, url_info, title)    # run_in_executor 사용
    return response

async def download_main(image_infos):
    title = image_infos[1]
    image_urls = image_infos[0]
    futures = [asyncio.ensure_future(download_fetch(url_info, title)) for url_info in image_urls]   # 태스크(퓨처) 객체를 리스트로 만듦
    result = await asyncio.gather(*futures)                # 결과를 한꺼번에 가져옴
    return result


try:
    
    init_dirs()
    target_url = input("텀블러 URL을 정확하게 입력하세요. : ")
    find_begin = time()
    html_text = get_html(target_url)
    image_infos = get_image_urls(html_text)
    loop = asyncio.get_event_loop() # 이벤트 루프를 얻음
    # image_urls = loop.run_until_complete(find_main(urls))   # main이 끝날 때까지 기다림
    find_end = time()
    download_begin = time()

    found_image_count = 0
    if (len(image_infos[0])) > 0 :
        downloaded_file_infos = loop.run_until_complete(download_main(image_infos))  # main이 끝날 때까지 기다림
        loop.close()

        download_success_file_list = list(filter(lambda x:x != 0, downloaded_file_infos))
        found_image_count = len(download_success_file_list)
        # print(download_success_file_list)

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

except Exception as error:
    print("Failed to Download Medias, error :  {}".format(error))

finally:
    print("Work Terminated.")
    

