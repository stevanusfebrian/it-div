import os
import platform
import pandas as pd
import numpy as np
import time
import asyncio
import threading
import json
import itertools
import nest_asyncio
nest_asyncio.apply()
from bs4 import BeautifulSoup

# for logging
import sys
import logging
import datetime
from logging.handlers import TimedRotatingFileHandler

from urllib.request import urlopen
from urllib.parse import urlparse, parse_qs

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

#logging
class PrintLogger:
    def __init__(self, log):
        self.terminal = sys.stdout
        self.log = log

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass

current_date = datetime.datetime.now().strftime("%Y-%m-%d")
def setup_logging():
    log_formatter = logging.Formatter("%(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    log_file = f'verval_scrape_{current_date}.log'
    log_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30, utc=False)
    log_handler.setFormatter(log_formatter)
    log_handler.setLevel(logging.DEBUG)
    
    logger = logging.getLogger()
    logger.addHandler(log_handler)

    sys.stdout = PrintLogger(log_handler.stream)
setup_logging()

os_system = platform.system()
print('OS SYSTEM:   ', os_system)

#cpu count
num_threads = os.cpu_count()
print(f'Num Threads:    {num_threads}')

# set path ke file chromedriver to operate the Chrome browser.
chrome_version = 'v116_0_5845_96'
if os_system == 'Windows':
    chrome_path = os.path.join('webdriver', 'chrome', os_system, chrome_version, 'chromedriver.exe')
elif os_system == 'Linux':
    chrome_path = os.path.join('webdriver', 'chrome', os_system, chrome_version, 'chromedriver')
else:
    chrome_path = os.path.join('webdriver', 'chrome', 'MacOS', chrome_version, 'chromedriver')

print('CHROME PATH:    ', chrome_path)
#webdriver options
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-setuid-sandbox')
#overcome limited resource problems
# chrome_options.add_argument('--disable-dev-shm-usage')
#open Browser in maximized mode
chrome_options.add_argument("start-maximized")
#disable extension
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

def driversetup():
  # webdriver_service = ChromeService(ChromeDriverManager().install())
  chrome_service = Service(executable_path=chrome_path)
  driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
  return driver
driver = driversetup()

#create empty dataframe
df_rekapitulasi = df_provinsi = df_kabupaten = df_kecamatan = df_sekolah_id_nama = pd.DataFrame()
kode_wilayah_kabupaten = pd.Series([]) 
# kode_wilayah_kecamatan = pd.Series([])
kode_wilayah_kecamatan = []
sekolah_id_enkrip = pd.Series([])
semester_label = pd.Series([])
semester_period = pd.Series([])
max_retries = 3

province_api = 'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=0&kode_wilayah=000000&semester_id=20222'
provinces = pd.read_json(province_api)
provinces = provinces.sort_values('kode_wilayah')
provinces = provinces['kode_wilayah']
provinces = provinces.astype(str).str.zfill(6)

# """## get kode wilayah `kabupaten`"""
# start_kab_kota = time.time()
# for province in provinces:
#     ##Get kode_wilayah kabupaten/kota list dari response api level 1
#     province = province.strip()
#     kab_api = 'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=1&kode_wilayah={0}&semester_id=20231'.format(province)

#     ##Get kode_wilayah kecamatan list dari response api level 2
#     kabupatens = pd.read_json(kab_api)
#     kabupatens = kabupatens.sort_values('kode_wilayah')
#     kabupatens = kabupatens['kode_wilayah']
#     kode_wilayah_kabupaten = kode_wilayah_kabupaten._append(kabupatens, ignore_index=True)

# kode_wilayah_kabupaten = kode_wilayah_kabupaten.astype(str).str.zfill(6)
# df_kode_wilayah_kabupaten = pd.DataFrame({'kode_wilayah_kabupaten': kode_wilayah_kabupaten})
# df_kode_wilayah_kabupaten.to_csv('./dataset/kode_wilayah_kabupaten.csv', index=False)
# print('kode_wilayah_kabupaten: ', len(kode_wilayah_kabupaten))
# print(f'get kab/kota done in: {time.time() - start_kab_kota} seconds')

"""## get kode wilayah `Kecamatan`"""
start_kecamatan = time.time()
print('getting kecamatan')
kode_wilayah_kabupaten = pd.read_csv('dataset\kode_wilayah_kabupaten.csv')
kode_wilayah_kabupaten = kode_wilayah_kabupaten['kode_wilayah_kabupaten']
kode_wilayah_kabupaten = kode_wilayah_kabupaten.astype(str).str.zfill(6)

# kode_wilayah_kabupaten = kode_wilayah_kabupaten[:32]

def get_kecamatan(kode_wilayah_kabupaten):
    for kabupaten in kode_wilayah_kabupaten:
        retry_count=0
        while retry_count < max_retries:
            try:
                kabupaten = kabupaten.strip()
                kec_api = f'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=2&kode_wilayah={kabupaten}&semester_id=20221'
                kecamatans = pd.read_json(kec_api)
                kecamatans = kecamatans.sort_values('kode_wilayah')
                kecamatans = kecamatans['kode_wilayah']
                # print(kecamatans)
                # kode_wilayah_kecamatan = kode_wilayah_kecamatan._append(kecamatans, ignore_index=True)
                kode_wilayah_kecamatan.append(kecamatans)
                # print(kode_wilayah_kecamatan)
                break
            except Exception as e:
                print(f'kode kabupaten: {kabupaten}, error: {e}, retry: {retry_count}')
                if retry_count == 2:
                    print(f'kecamatan with kabupaten code {kabupaten} was failed to be get')
                retry_count += 1
                time.sleep(3)
    return None

def main_get_kode_kecamatan():
    threads = []
    yayasan_batches = np.array_split(kode_wilayah_kabupaten, num_threads)
    for t in range(num_threads):
        thread = threading.Thread(target=get_kecamatan, args=(yayasan_batches[t],))
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    return None

main_get_kode_kecamatan()
kode_wilayah_kecamatan = pd.concat(kode_wilayah_kecamatan, ignore_index=True)

# for kabupaten in kode_wilayah_kabupaten:
#     retry_count=0
#     while retry_count < max_retries:
#         try:
#             kabupaten = kabupaten.strip()
#             kec_api = f'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=2&kode_wilayah={kabupaten}&semester_id=20221'
#             kecamatans = pd.read_json(kec_api)
#             kecamatans = kecamatans.sort_values('kode_wilayah')
#             kecamatans = kecamatans['kode_wilayah']
#             kode_wilayah_kecamatan = kode_wilayah_kecamatan._append(kecamatans, ignore_index=True)
#             break
#         except Exception as e:
#             print(f'kode kabupaten: {kabupaten}, error: {e}, retry: {retry_count}')
#             if retry_count == 2:
#                 print(f'kecamatan with kabupaten code {kabupaten} was failed to be get')
#             retry_count += 1
#             time.sleep(4)

kode_wilayah_kecamatan = kode_wilayah_kecamatan.astype(str).str.zfill(6)
df_kode_wilayah_kecamatan = pd.DataFrame({'kode_wilayah_kecamatan': kode_wilayah_kecamatan})
df_kode_wilayah_kecamatan.to_csv('./dataset/kode_wilayah_kecamatan_thread.csv', index=False)
print(f'get kecamatan done in: {time.time() - start_kecamatan} seconds')
print('kode_wilayah_kecamatan: ', len(kode_wilayah_kecamatan))


# """## get semester list"""
# kode_wilayah_kecamatan = pd.read_csv('dataset\kode_wilayah_kecamatan_thread.csv')
# kode_wilayah_kecamatan = kode_wilayah_kecamatan['kode_wilayah_kecamatan']
# kode_wilayah_kecamatan = kode_wilayah_kecamatan.astype(str).str.zfill(6)

# #contoh page kecamatan kemayoran. untuk ngambil tag select semester
# page_url = 'https://dapo.kemdikbud.go.id/pd/3/016006'
# page = driver.get(page_url)
# soup = BeautifulSoup(driver.page_source, 'html.parser')

# #get periode for rekapitulasi each semester
# semester_options = soup.find(id='selectSemester').find_all('option')[1:]
# semester_lists = [x.get('value') for x in semester_options]
# print('semester lists ', semester_lists)

# semester_titles = [x.get_text().strip() for x in semester_options]
# print('semester_titles ', semester_titles)

# """## generate rekapitulasi urls in kecamatan level"""
# start_rekap = time.time()
# rekapitulasi_urls = [{
#                     'urls': 'https://dapo.kemdikbud.go.id/rekap/progresSP?id_level_wilayah=3&kode_wilayah={0}&semester_id={1}'.format(kode, semester)}
#                     for kode, semester
#                     in itertools.product(kode_wilayah_kecamatan, semester_lists)]

# rekapitulasi_urls = pd.DataFrame(rekapitulasi_urls)
# rekapitulasi_urls.to_csv('./dataset/rekapitulasi_urls.csv', index=False)
# print(f'generate rekapitulasi url done in: {time.time() - start_rekap} seconds')
# print('rekapitulasi_urls: ', len(rekapitulasi_urls))

# """## get rekapitulasi secara asynchronous"""
# rekapitulasi_urls = pd.read_csv('dataset\\rekapitulasi_urls.csv')
# batches_rekapitulasi_urls = np.array_split(rekapitulasi_urls, 30)

# school_profiles_list = []
# unprocessed_profiles_list = []

# async def fetch(url, semaphore, max_retries=3, retry_delay=1):
#     try_count = 0
#     while try_count < max_retries:
#         try:
#             async with semaphore:
#                 semester_id = parse_qs(urlparse(url).query)['semester_id'][0]
#                 response = await event_loop.run_in_executor(None, urlopen, url)
#                 data = await event_loop.run_in_executor(None, response.read)
#                 data = json.loads(data)
#                 data = [{**item, 'semester_id': semester_id} for item in data]
#                 school_profiles_list.append(pd.DataFrame(data))
#                 return
#         except Exception as e:
#             print(f"Error occurred for URL: {url}, MSG: {e}")
#             try_count += 1
#             if try_count < max_retries:
#                 print(f"Retrying connection after {retry_delay} second(s)...")
#                 await asyncio.sleep(retry_delay)
#             else:
#                 unprocessed_profiles_list.append(url)
#                 print(f"Maximum retry attempts reached for URL: {url}")
#                 return None


# async def main(urls):
#     combined_dict = []
#     semaphore = asyncio.Semaphore(100)
#     tasks = [fetch(url, semaphore) for url in urls]
#     responses = await asyncio.gather(*tasks)
#     for sublist in responses:
#         combined_dict.extend(sublist)
#     return combined_dict

# if __name__ == '__main__':
#     for batch in batches_rekapitulasi_urls:
#         results = asyncio.run(main(batch))

#     unprocessed_profiles_list = [*set(unprocessed_profiles_list)] #remove duplicate
#     if unprocessed_profiles_list:
#         asyncio.run(main(unprocessed_profiles_list))
#         unprocessed_profiles_list = [*set(unprocessed_profiles_list)] #remove duplicate

# school_profiles_result = pd.concat(school_profiles_list, ignore_index=True)

# school_profiles_result.to_csv(f'./dataset/result_school_profiles_{current_date}.csv', encoding='utf-8', index=False)