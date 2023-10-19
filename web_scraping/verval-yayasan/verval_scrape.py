# -*- coding: utf-8 -*-
import os
import platform
import pandas as pd
import numpy as np
import time
import threading
import aiohttp
import asyncio
import nest_asyncio
nest_asyncio.apply()

# for logging
import sys
import logging
import datetime
from logging.handlers import TimedRotatingFileHandler

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

# timeout limit for WebDriverWait
timeout_limit = 30

os_system = platform.system()
print('OS SYSTEM:   ', os_system)

#cpu count
cores = os.cpu_count()
print(f'CPU CORES:    {cores}')

# set path ke file chromedriver to operate the Chrome browser.
chrome_version = 'v114_0_5735_90'
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

"""## get provinsi"""

#get provinsi
start_province = time.time()
driver = driversetup()
driver.get('https://vervalyayasan.data.kemdikbud.go.id/index.php/Chome/rekapitulasi?kode_wilayah=000000')

#to show all link (dropdown)
dropdown_container = driver.find_element(By.CLASS_NAME, 'dataTables_length')
select = dropdown_container.find_element(By.TAG_NAME, 'select')
dropdown = Select(select)
select_all = dropdown.select_by_value('-1')

province_list = []
province_urls = []

urls_elements = driver.find_element(By.TAG_NAME, 'tbody').find_elements(By.XPATH, "tr/td/a")

for url in urls_elements:
  province_list.append(url.get_attribute('innerHTML'))
  province_urls.append(url.get_attribute('href'))

df_provinces = pd.DataFrame({'province': province_list, 'link': province_urls})
df_provinces.to_csv('./dataset/province_list.csv', index=False)
print(f'province done in: {time.time() - start_province} seconds')

"""## kabupaten/kota links asynchronously"""
start_kabupaten = time.time()
kab_kota_list = []
kab_kota_urls = []

async def get_kab_kota_urls(url, max_retries=3):
  retry_count = 0
  while(retry_count < max_retries):
    try:
      driver.get(url)

      #navigate dropdown to show all link
      dropdown_container = driver.find_element(By.CLASS_NAME, 'dataTables_length')
      select = dropdown_container.find_element(By.TAG_NAME, 'select')
      dropdown = Select(select)
      select_all = dropdown.select_by_value('-1')

      urls_elements = driver.find_element(By.TAG_NAME, 'tbody').find_elements(By.XPATH, "tr/td/a")

      for url in urls_elements:
        kab_kota_list.append(url.get_attribute('innerHTML'))
        kab_kota_urls.append(url.get_attribute('href'))
      return

    except Exception as e:
      print(f'{url}, error: {e}')
      retry_count += 1
      await asyncio.sleep(1)
  return

async def get_all_kab_kota_urls(urls):
  tasks = []
  for url in urls:
      task = asyncio.create_task(get_kab_kota_urls(url))
      tasks.append(task)
  await asyncio.gather(*tasks)
  return

async def main_kab_kota(urls):
  await get_all_kab_kota_urls(urls)
  return

if __name__ == '__main__':
  asyncio.run(main_kab_kota(province_urls))

df_kab_kota = pd.DataFrame({'kab/kota': kab_kota_list, 'link': kab_kota_urls})
print(df_kab_kota['link'])
df_kab_kota.to_csv('./dataset/kab_kota_list.csv', index=False)
print(f'kabupaten done in: {time.time() - start_kabupaten}')

"""## kecamatan asynchronously"""
start_kecamatan = time.time()
kecamatan_list = []
kecamatan_urls = []

async def get_kecamatan_urls(url, max_retries=3):
  retry_count = 0
  while(retry_count < max_retries):
    try:

      driver.get(url)

      #navigate dropdown to show all url
      dropdown_container = driver.find_element(By.CLASS_NAME, 'dataTables_length')
      select = dropdown_container.find_element(By.TAG_NAME, 'select')
      dropdown = Select(select)
      select_all = dropdown.select_by_value('-1')

      urls_elements = driver.find_element(By.TAG_NAME, 'tbody').find_elements(By.XPATH, "tr/td/a")

      for url in urls_elements:
        kecamatan_list.append(url.get_attribute('innerHTML'))
        kecamatan_urls.append(url.get_attribute('href'))
      return
    except Exception as e:
      print(f'{url}, error: {e}')
      retry_count += 1
      await asyncio.sleep(1)
  return

async def get_all_kecamatan_urls(urls):
  tasks = []
  for url in urls:
      task = asyncio.create_task(get_kecamatan_urls(url))
      tasks.append(task)
  await asyncio.gather(*tasks)
  return

async def main_kecamatan(urls):
  await get_all_kecamatan_urls(urls)
  return

if __name__ == '__main__':
  asyncio.run(main_kecamatan(kab_kota_urls))


df_kecamatan = pd.DataFrame({'kecamatan': kecamatan_list, 'url': kecamatan_urls})
df_kecamatan.to_csv('./dataset/df_kecamatan_verval.csv', index=False)
print(f'get kecamatan done in: {time.time() - start_kecamatan}')

"""## get yayasan"""
start_yayasan = time.time()
df_kecamatan = pd.read_csv('../dataset/df_kecamatan_verval.csv') # read karna biar ga scrape dari web lagi
kecamatan_urls = df_kecamatan['url']

yayasan_list = []
yayasan_urls = []

#yayasan list
async def get_yayasan(url, semaphore, max_retries=1):
  retry_count = 0
  while(retry_count < max_retries):
    try:
      async with semaphore:
        driver.get(url)

        #navigate dropdown to show all link
        dropdown_container = driver.find_element(By.CLASS_NAME, 'dataTables_length')
        select = dropdown_container.find_element(By.TAG_NAME, 'select')
        dropdown = Select(select)
        select_all = dropdown.select_by_value('-1')

        #get NPYP url column
        urls_elements = driver.find_element(By.TAG_NAME, 'tbody').find_elements(By.XPATH, "tr/td/a")
        for url in urls_elements:
          yayasan_list.append(url.get_attribute('innerHTML'))
          yayasan_urls.append(url.get_attribute('href'))

        return

    except Exception as e:
      print(f'{url}, error: {e}')
      retry_count += 1
      await asyncio.sleep(1)
  return

async def get_all_yayasan(urls, semaphore):
  tasks = []
  for url in urls:
    task = asyncio.create_task(get_yayasan(url, semaphore))
    tasks.append(task)
  await asyncio.gather(*tasks)
  return

async def main_yayasan(urls, semaphore):
  await get_all_yayasan(urls, semaphore)
  return

if __name__ == '__main__':
  start = time.time()
  semaphore = asyncio.Semaphore(500)
  asyncio.run(main_yayasan(kecamatan_urls, semaphore))
  print('time: ', time.time() - start)

df_yayasan = pd.DataFrame({'yayasan': yayasan_list, 'url': yayasan_urls})

df_yayasan.to_csv('./dataset/df_yayasan.csv', index=False)
print(f'get yayasan done in: {time.time() - start_yayasan}')

"""## get profile yayasan"""
# df_yayasan = pd.read_csv('https://raw.githubusercontent.com/stevanusfebrian/it-div/main/scraping/verval/df_verval_yayasan.csv')
# df_yayasan = df_yayasan['url']
# df_yayasan = df_yayasan[12000:20000]

# df_yayasan = pd.read_csv('./urls_in_cabang_yayasan.csv')
df_yayasan = df_yayasan['url']
partition = 4
yayasan_partition = np.array_split(df_yayasan, partition)

df_yayasan1 = yayasan_partition[0]
df_yayasan2 = yayasan_partition[1]
df_yayasan3 = yayasan_partition[2]
df_yayasan4 = yayasan_partition[3]

profile_yayasan_list1 = []
profile_yayasan_list2 = []
profile_yayasan_list3 = []
profile_yayasan_list4 = []

additional_profile_yayasan_list1 = []
additional_profile_yayasan_list2 = []
additional_profile_yayasan_list3 = []
additional_profile_yayasan_list4 = []

failed_yayasan = [] #yayasan yang gagal di scrape
additional_failed_yayasan = [] #additional yayasan yang gagal di scrape

# """## Scrape profile Threading synchronous"""

# #profile yayasan
# def make_profile_yayasan_dict():
#   profile_yayasan_dict = {
#     'Pimpinan Yayasan': [],
#     'Operator Yayasan': [],
#     'Telepon Yayasan': [],
#     'Fax Yayasan': [],
#     'Email Yayasan': [],
#     'Kode Pos Yayasan': [],
#     'No Pendirian Yayasan': [],
#     'Tanggal Pendirian Yayasan': [],
#     'No Pengesahan PN LN Yayasan': [],
#     'No Pengesahan Menkumham Yayasan': [],
#     'Tanggal Pengesahan Menkumham Yayasan': [],
#     'Nama Yayasan': [],
#     'Kode Yayasan': [],
#   }
#   return profile_yayasan_dict

# def parse_yayasan(url, driver, profile_yayasan_list, max_retries=3):
#   retry_count=0
#   while retry_count < max_retries:
#     try:
#       driver.get(url)

#       # check if url is loaded but there's no content
#       check_content = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.CLASS_NAME, 'box')))
#       if check_content:
#         # print(f'content found at try: {retry_count}, {url[-36:]}')
#         select = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.NAME, 'tabelsekolah_length')))
#         if select:
#           print(f'table   found at try: {retry_count}, {url[-36:]}')
#           #1. cari dropdown filter untuk select = 'all'
#           # select = driver.find_element(By.NAME, 'tabelsekolah_length')
#           dropdown = Select(select)
#           dropdown.select_by_value('-1')

#           raw = driver.page_source
#           soup = BeautifulSoup(raw, 'html.parser') # untuk parsing

#           #2.bagian table sekolah naungan
#           #2.1 read table sekolah naungan
#           read_sekolah_naungan = pd.read_html(raw)
#           sekolah_naungan = read_sekolah_naungan[0]

#           #2.2 link sekolah naungan
#           urls_elements = soup.find('tbody').find_all('a', href=True)
#           if urls_elements:
#             urls_npsn = [url['href'] for url in urls_elements]
#             url_dict = {'url': urls_npsn}
#           else:
#             url_dict = {'url': None}
#           sekolah_naungan = sekolah_naungan.assign(URL=url_dict['url'])

#           #3. parsing profile yayasan
#           #3.1 bikin dictionary kosong for each profile
#           profile_yayasan_dict = make_profile_yayasan_dict()

#           #3.2 header yayasan
#           header = soup.select_one('h4', attrs={'class': 'page-header'}).get_text().strip() #get all text in h4
#           address = soup.select_one('small font', attrs={'class': 'small'}).get_text().strip() #get address only
#           yayasan = header.split(f' {address}')[0]
#           yayasan = yayasan.split(' ', 1)

#           nama_yayasan = yayasan[1]
#           kode_yayasan = yayasan[0][1:len(yayasan[0])-1]

#           #3.3 profile yayasan
#           li_profil_yayasan = soup.select_one('ul', attrs={'class': 'list-group'}).find_all('li')
#           profile_yayasan = [li.get_text().split(' : ')[1] for li in li_profil_yayasan]
#           profile_yayasan.append(nama_yayasan)
#           profile_yayasan.append(kode_yayasan)
#           for i, (key, value) in enumerate(profile_yayasan_dict.items()):
#             value.append(profile_yayasan[i])

#           #4. make repeated profile yayasan to be inserted to sekolah_naungan
#           repeated_profile = {key: value * len(sekolah_naungan) for key, value in profile_yayasan_dict.items()}
#           profile_and_schools = sekolah_naungan.assign(**repeated_profile)
#           #5. append dataframes
#           # exec(f'profile_yayasan_list{i+1}.append(profile_and_schools)')
#           profile_yayasan_list.append(profile_and_schools)
#           return
#         else:
#           print(f'table   unavailable at try: {retry_count}, {url[-36:]}')
#           retry_count += 1
#           time.sleep(2)
#           continue
#       else:
#         print(f'content unavailable at try: {retry_count}, {url[-36:]}')
#         retry_count += 1
#         time.sleep(2)
#         continue

#     except aiohttp.ClientError as e:
#       failed_yayasan.append(url)
#       print(f'{url}, try: {retry_count}, {url}, error: {e}')
#       retry_count += 1
#       time.sleep(2)
#       continue

#     except Exception as e:
#       # Handle the exception here without stopping the script
#       print(f'exception error     at try: {retry_count}, {url[-36:]},  {e}')
#       if retry_count == 2:
#         failed_yayasan.append(url)
#       retry_count += 1
#       time.sleep(2)
#       continue
#   return None

# def parse_all_yayasan(urls, profile_yayasan_list):
#   driver = driversetup()
#   for url in urls:
#     parse_yayasan(url, driver, profile_yayasan_list)
#   driver.quit()

# def main_prof_yayasan(yayasan, profile_yayasan_list):
#   num_threads = cores
#   batches_df_yayasan = np.array_split(yayasan, num_threads)

#   threads = []
#   for t in range(num_threads):
#     thread = threading.Thread(target=parse_all_yayasan, args=(batches_df_yayasan[t], profile_yayasan_list))
#     threads.append(thread)

#   for thread in threads:
#     thread.start()

#   for thread in threads:
#     thread.join()

# # jalanin scraping per partisi
# process_prof_yayasan_time = time.time()
# main_prof_yayasan(df_yayasan1, profile_yayasan_list1)
# main_prof_yayasan(df_yayasan2, profile_yayasan_list2)
# main_prof_yayasan(df_yayasan3, profile_yayasan_list3)
# main_prof_yayasan(df_yayasan4, profile_yayasan_list4)


# def process_additional_yayasan(profile_yayasan_list, additional_profile_yayasan_list, file_name):
#   df_profile_yayasan_school = pd.concat(profile_yayasan_list, ignore_index=True)
#   # if there's no NPYP column then save it
#   if 'NPYP' not in df_profile_yayasan_school:
#     df_profile_yayasan_school.to_csv(f'./dataset/{file_name}_{current_date}.csv', index=False)
#     print(f'processing {file_name} in: {time.time() - process_prof_yayasan_time} seconds')
#   else:
#     def parse_additional_yayasan(url, driver, additional_profile_yayasan_list, max_retries=3):
#       retry_count=0
#       while retry_count < max_retries:
#         try:
#           driver.get(url)
#           # check if url is loaded but there's no content
#           check_content = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.CLASS_NAME, 'box')))
#           if check_content:
#             print(f'additional content found at try: {retry_count}, {url[-36:]}')
#             select = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.NAME, 'tabelsekolah_length')))
#             if select:
#               print(f'additional table   found at try: {retry_count}, {url[-36:]}')
#               #1. cari dropdown filter untuk select = 'all'
#               # select = driver.find_element(By.NAME, 'tabelsekolah_length')
#               dropdown = Select(select)
#               dropdown.select_by_value('-1')

#               raw = driver.page_source
#               soup = BeautifulSoup(raw, 'html.parser') # untuk parsing

#               #2.bagian table sekolah naungan
#               #2.1 read table sekolah naungan
#               read_sekolah_naungan = pd.read_html(raw)
#               sekolah_naungan = read_sekolah_naungan[0]

#               #2.2 link sekolah naungan
#               urls_elements = soup.find('tbody').find_all('a', href=True)
#               if urls_elements:
#                 urls_npsn = [url['href'] for url in urls_elements]
#                 url_dict = {'url': urls_npsn}
#               else:
#                 url_dict = {'url': None}
#               sekolah_naungan = sekolah_naungan.assign(URL=url_dict['url'])

#               #3. parsing profile yayasan
#               #3.1 bikin dictionary kosong for each profile
#               profile_yayasan_dict = make_profile_yayasan_dict()

#               #3.2 header yayasan
#               header = soup.select_one('h4', attrs={'class': 'page-header'}).get_text().strip() #get all text in h4
#               address = soup.select_one('small font', attrs={'class': 'small'}).get_text().strip() #get address only
#               yayasan = header.split(f' {address}')[0]
#               yayasan = yayasan.split(' ', 1)

#               nama_yayasan = yayasan[1]
#               kode_yayasan = yayasan[0][1:len(yayasan[0])-1]

#               #3.3 profile yayasan
#               li_profil_yayasan = soup.select_one('ul', attrs={'class': 'list-group'}).find_all('li')
#               profile_yayasan = [li.get_text().split(' : ')[1] for li in li_profil_yayasan]
#               profile_yayasan.append(nama_yayasan)
#               profile_yayasan.append(kode_yayasan)
#               for i, (key, value) in enumerate(profile_yayasan_dict.items()):
#                 value.append(profile_yayasan[i])

#               #4. make repeated profile yayasan to be inserted to sekolah_naungan
#               repeated_profile = {key: value * len(sekolah_naungan) for key, value in profile_yayasan_dict.items()}
#               profile_and_schools = sekolah_naungan.assign(**repeated_profile)
#               #5. append dataframes
#               additional_profile_yayasan_list.append(profile_and_schools)
#               return
#             else:
#               print(f'additional table   unavailable at try: {retry_count}, {url[-36:]}')
#               retry_count += 1
#               time.sleep(2)
#               continue
#           else:
#             print(f'additional content unavailable at try: {retry_count}, {url[-36:]}')
#             retry_count += 1
#             time.sleep(2)
#             continue

#         except aiohttp.ClientError as e:
#           if retry_count == 2:
#             additional_failed_yayasan.append(url)
#           print(f'{url[-36:]}, error: {e}')
#           retry_count += 1
#           time.sleep(1)

#         except Exception as e:
#           # Handle the exception here without stopping the script
#           print(f'additional exception error     at try: {retry_count}, {url[-36:]},  {e}')
#           if retry_count == 2:
#             additional_failed_yayasan.append(url)
#           retry_count += 1
#           time.sleep(2)
#           continue
#       return None

#     def parse_all_additional_yayasan(urls, additional_profile_yayasan_list):
#       driver = driversetup()
#       for url in urls:
#         parse_additional_yayasan(url, driver, additional_profile_yayasan_list)

#     def main_additional_prof_yayasan(yayasan, additional_profile_yayasan_list):
#       num_threads = cores
#       batches_df_yayasan = np.array_split(yayasan, num_threads)

#       threads = []
#       for t in range(num_threads):
#         thread = threading.Thread(target=parse_all_additional_yayasan, args=(batches_df_yayasan[t], additional_profile_yayasan_list))
#         threads.append(thread)

#       for thread in threads:
#         thread.start()

#       for thread in threads:
#         thread.join()

#     process_additional_prof_yayasan_time = time.time()
#     additional_yayasan = df_profile_yayasan_school[df_profile_yayasan_school['NPYP'].notnull()]['URL']

#     # scrape the additional yayasan
#     main_additional_prof_yayasan(additional_yayasan, additional_profile_yayasan_list)

#     # check the additional yayasan, then save it
#     if len(additional_profile_yayasan_list) == 0:
#       print('empty additional_profile_yayasan dataframe')
#     else:
#       df_additional_profile_yayasan_school = pd.concat(additional_profile_yayasan_list, ignore_index=True)

#       # gabung profile yayasan dan additional profile yayasan
#       combined_profile_list = [df_profile_yayasan_school, df_additional_profile_yayasan_school]
#       result_profile_yayasan_school_df = pd.concat(combined_profile_list, ignore_index=True)

#       # pindahin column dataframe
#       column_to_move1 = result_profile_yayasan_school_df.pop("Nama Yayasan")
#       result_profile_yayasan_school_df.insert(7, "Nama Yayasan", column_to_move1)
#       column_to_move2 = result_profile_yayasan_school_df.pop("Kode Yayasan")
#       result_profile_yayasan_school_df.insert(8, "Kode Yayasan", column_to_move2)

#       # check if masih ada yayasan yang belum di visit, kalau tidak ada drop kolom npyp
#       if len(result_profile_yayasan_school_df[result_profile_yayasan_school_df['NPYP'].notnull()]) == 0:
#         result_profile_yayasan_school_df = result_profile_yayasan_school_df.drop(['NPYP'], axis=1)

#       result_profile_yayasan_school_df.to_csv(f'./dataset/{file_name}_{current_date}.csv', index=False)
#       print(f'processing additional {file_name} in: {time.time() - process_additional_prof_yayasan_time} seconds')

# def convert_profile_yayasan_to_df(profile_yayasan_list, additional_profile_yayasan_list, file_name):
#   # check profile yayasnan list first, whether it's empty or not
#   if len(profile_yayasan_list) == 0:
#     print('empty profile yayasan')
#   else:
#     process_additional_yayasan(profile_yayasan_list, additional_profile_yayasan_list, file_name)

# convert_profile_yayasan_to_df(profile_yayasan_list1, additional_profile_yayasan_list1, 'profile_yayasan_list_1')
# convert_profile_yayasan_to_df(profile_yayasan_list2, additional_profile_yayasan_list2, 'profile_yayasan_list_2')
# convert_profile_yayasan_to_df(profile_yayasan_list3, additional_profile_yayasan_list3, 'profile_yayasan_list_3')
# convert_profile_yayasan_to_df(profile_yayasan_list4, additional_profile_yayasan_list4, 'profile_yayasan_list_4')

# if failed_yayasan:
#   print('list of yayasan failed to be scrape')
#   for yayasan in failed_yayasan:
#     print(yayasan)
# else:
#   print('no yayasan failed to be scrape')

# if additional_failed_yayasan:
#   print('list of additional yayasan failed to be scrape')
#   for yayasan in additional_failed_yayasan:
#     print(yayasan)
# else:
#   print('no additional yayasan failed to be scrape')


"""# PEMBATAS"""


# if len(profile_yayasan_list) == 0: # empty dataframe
#   print('empty profile yayasan')
#   print(f'get profile yayasan done (empty) in: {time.time() - start_prof_yayasan}')
# else:
#   df_profile_yayasan_school = pd.concat(profile_yayasan_list, ignore_index=True)
#   print(f'get profile yayasan done in: {time.time() - start_prof_yayasan}')
#   # df_profile_yayasan_school.to_csv('./dataset/result_profile_yayasan.csv', index=False)

#   """## check additional profile yayasan"""
#   #dalem df_profile_yayasan_school ada isi profile yayasan yg ngetable yayasan lagi
#   start_add_prof_yayasan = time.time()
#   if 'NPYP' not in df_profile_yayasan_school:
#     # directly create final profile yayasan if there's no additional npyp
#     df_profile_yayasan_school.to_csv(f'./dataset/result_profile_yayasan_{current_date}.csv', index=False)
#   else:
#     additional_yayasan = df_profile_yayasan_school[df_profile_yayasan_school['NPYP'].notnull()]['URL']

#     def parse_additional_yayasan(url, driver, max_retries=3):
#       retry_count=0
#       while retry_count < max_retries:
#         try:
#           driver.get(url)
#           # check if url is loaded but there's no content
#           check_content = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.CLASS_NAME, 'box')))
#           if check_content:
#             print(f'additional content found at try: {retry_count}, {url[-36:]}')
#             select = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.NAME, 'tabelsekolah_length')))
#             if select:
#               print(f'additional table   found at try: {retry_count}, {url[-36:]}')
#               #1. cari dropdown filter untuk select = 'all'
#               # select = driver.find_element(By.NAME, 'tabelsekolah_length')
#               dropdown = Select(select)
#               dropdown.select_by_value('-1')

#               raw = driver.page_source
#               soup = BeautifulSoup(raw, 'html.parser') # untuk parsing

#               #2.bagian table sekolah naungan
#               #2.1 read table sekolah naungan
#               read_sekolah_naungan = pd.read_html(raw)
#               sekolah_naungan = read_sekolah_naungan[0]

#               #2.2 link sekolah naungan
#               urls_elements = soup.find('tbody').find_all('a', href=True)
#               if urls_elements:
#                 urls_npsn = [url['href'] for url in urls_elements]
#                 url_dict = {'url': urls_npsn}
#               else:
#                 url_dict = {'url': None}
#               sekolah_naungan = sekolah_naungan.assign(URL=url_dict['url'])

#               #3. parsing profile yayasan
#               #3.1 bikin dictionary kosong for each profile
#               profile_yayasan_dict = make_profile_yayasan_dict()

#               #3.2 header yayasan
#               header = soup.select_one('h4', attrs={'class': 'page-header'}).get_text().strip() #get all text in h4
#               address = soup.select_one('small font', attrs={'class': 'small'}).get_text().strip() #get address only
#               yayasan = header.split(f' {address}')[0]
#               yayasan = yayasan.split(' ', 1)

#               nama_yayasan = yayasan[1]
#               kode_yayasan = yayasan[0][1:len(yayasan[0])-1]

#               #3.3 profile yayasan
#               li_profil_yayasan = soup.select_one('ul', attrs={'class': 'list-group'}).find_all('li')
#               profile_yayasan = [li.get_text().split(' : ')[1] for li in li_profil_yayasan]
#               profile_yayasan.append(nama_yayasan)
#               profile_yayasan.append(kode_yayasan)
#               for i, (key, value) in enumerate(profile_yayasan_dict.items()):
#                 value.append(profile_yayasan[i])

#               #4. make repeated profile yayasan to be inserted to sekolah_naungan
#               repeated_profile = {key: value * len(sekolah_naungan) for key, value in profile_yayasan_dict.items()}
#               profile_and_schools = sekolah_naungan.assign(**repeated_profile)
#               #5. append dataframes
#               additional_profile_yayasan_list.append(profile_and_schools)
#               return
#             else:
#               print(f'additional table   unavailable at try: {retry_count}, {url[-36:]}')
#               retry_count += 1
#               time.sleep(2)
#               continue
#           else:
#             print(f'additional content unavailable at try: {retry_count}, {url[-36:]}')
#             retry_count += 1
#             time.sleep(2)
#             continue

#         except aiohttp.ClientError as e:
#           additional_unprocessed_yayasan.append(url)
#           print(f'{url[-36:]}, error: {e}')
#           retry_count += 1
#           time.sleep(1)

#         except Exception as e:
#           # Handle the exception here without stopping the script
#           print(f'additional exception error     at try: {retry_count}, {url[-36:]},  {e}')
#           retry_count += 1
#           time.sleep(2)
#           continue
#       return None

#     def parse_all_additional_yayasan(urls):
#       driver = driversetup()
#       for url in urls:
#         task = parse_additional_yayasan(url, driver)

#     def main_additional_prof_yayasan(yayasan):
#       num_threads = cores
#       batches_df_yayasan = np.array_split(yayasan, num_threads)

#       threads = []
#       for t in range(num_threads):
#         thread = threading.Thread(target=parse_all_additional_yayasan, args=[batches_df_yayasan[t]])
#         threads.append(thread)

#       for thread in threads:
#         thread.start()

#       for thread in threads:
#         thread.join()

#     main_additional_prof_yayasan(additional_yayasan)
#     if additional_unprocessed_yayasan:
#       main_additional_prof_yayasan(additional_unprocessed_yayasan)
#       #remove duplicate
      # additional_unprocessed_yayasan = [*set(additional_unprocessed_yayasan)]

#     if len(additional_profile_yayasan_list) == 0:
#       # misal ada additional yayasan tapi tetap ga dapet data (misal karena error: content unavailable
#       # langsung save df_profile_yayasan_school
#       print('empty additional_profile_yayasan dataframe')
#       print(f'get additional profile yayasan (empty) in: {time.time() - start_add_prof_yayasan}')
#       df_profile_yayasan_school.to_csv(f'./dataset/result_profile_yayasan_{current_date}.csv', index=False)
#     else:
#       df_additional_profile_yayasan_school = pd.concat(additional_profile_yayasan_list, ignore_index=True)
#       print(f'get additional profile yayasan in: {time.time() - start_add_prof_yayasan}')

#       # gabung profile yayasan dan additional profile yayasan
#       combined_profile_list = [df_profile_yayasan_school, df_additional_profile_yayasan_school]
#       result_profile_yayasan_school_df = pd.concat(combined_profile_list, ignore_index=True)

#       # pindahin column dataframe
#       column_to_move1 = result_profile_yayasan_school_df.pop("Nama Yayasan")
#       result_profile_yayasan_school_df.insert(7, "Nama Yayasan", column_to_move1)
#       column_to_move2 = result_profile_yayasan_school_df.pop("Kode Yayasan")
#       result_profile_yayasan_school_df.insert(8, "Kode Yayasan", column_to_move2)

#       # check if masih ada yayasan yang belum di visit, kalau tidak ada drop kolom npyp
#       if len(result_profile_yayasan_school_df[result_profile_yayasan_school_df['NPYP'].notnull()]) == 0:
#         result_profile_yayasan_school_df = result_profile_yayasan_school_df.drop(['NPYP'], axis=1)

#       result_profile_yayasan_school_df.to_csv(f'./dataset/result_profile_yayasan_{current_date}.csv', index=False)

