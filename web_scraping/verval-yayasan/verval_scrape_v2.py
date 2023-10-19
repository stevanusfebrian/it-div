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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

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
  log_file = f'./loggings/verval_scrape_{current_date}.log'
  log_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30, utc=False)
  log_handler.setFormatter(log_formatter)
  log_handler.setLevel(logging.DEBUG)
  logger = logging.getLogger()
  logger.addHandler(log_handler)

  sys.stdout = PrintLogger(log_handler.stream)

setup_logging()

# timeout limit for WebDriverWait
timeout_limit = 5

os_system = platform.system()
print('OS SYSTEM:   ', os_system)

#cpu count
cores = os.cpu_count()
print(f'CPU CORES:    {cores}')

# set path ke file chromedriver to operate the Chrome browser.
# chrome_version = 'v114_0_5735_90'
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



"""## get provinsi"""

#get provinsi
print('getting provices')
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

df_provinces = pd.DataFrame({'province': province_list, 'urls': province_urls})
df_provinces.to_csv('./dataset/province_list.csv', index=False)
print(f'province done in: {time.time() - start_province} seconds')


"""## kabupaten/kota links asynchronously"""
print('getting kabupaten/kota')
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

df_kab_kota = pd.DataFrame({'kab/kota': kab_kota_list, 'urls': kab_kota_urls})
df_kab_kota.to_csv('./dataset/kab_kota_list.csv', index=False)
print(f'kabupaten done in: {time.time() - start_kabupaten}')

"""## kecamatan asynchronously"""
print('getting kecamatan')
kab_kota_urls = pd.read_csv('./dataset/kab_kota_list.csv')
kab_kota_urls = kab_kota_urls['urls']
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

df_kecamatan = pd.DataFrame({'kecamatan': kecamatan_list, 'urls': kecamatan_urls})
df_kecamatan.to_csv('./dataset/df_kecamatan_verval.csv', index=False)
print(f'get kecamatan done in: {time.time() - start_kecamatan}')


"""## get yayasan"""
print('getting yayasan')
start_yayasan = time.time()
df_kecamatan = pd.read_csv('./dataset/df_kecamatan_verval.csv') # read karna biar ga scrape dari web lagi
kecamatan_urls = df_kecamatan['urls']

yayasan_list = []
yayasan_urls = []

#yayasan list
async def get_yayasan(url, semaphore, max_retries=3):
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

df_yayasan = pd.DataFrame({'yayasan': yayasan_list, 'urls': yayasan_urls})

df_yayasan.to_csv('./dataset/yayasan_list.csv', index=False)
print(f'get yayasan done in: {time.time() - start_yayasan}')


"""## Scrape profile dengan Threading"""
print('getting yayasan profiles')
# df_yayasan = pd.read_csv('https://raw.githubusercontent.com/stevanusfebrian/it-div/main/scraping/verval/df_verval_yayasan.csv')
df_yayasan = pd.read_csv('./dataset/yayasan_list.csv')
df_yayasan = df_yayasan['urls']
# df_yayasan = df_yayasan[12000:20003]
start_yayasan_profiles = time.time()

#profile yayasan
def make_profile_yayasan_dict():
  profile_yayasan_dict = {
    'Pimpinan Yayasan': [],
    'Operator Yayasan': [],
    'Telepon Yayasan': [],
    'Fax Yayasan': [],
    'Email Yayasan': [],
    'Kode Pos Yayasan': [],
    'No Pendirian Yayasan': [],
    'Tanggal Pendirian Yayasan': [],
    'No Pengesahan PN LN Yayasan': [],
    'No Pengesahan Menkumham Yayasan': [],
    'Tanggal Pengesahan Menkumham Yayasan': [],
    'Nama Yayasan': [],
    'Kode Yayasan': [],
  }
  return profile_yayasan_dict

def parse_profile_and_sekolah(raw, soup, profile_yayasan_list, index_table_sekolah):
  #2.bagian table sekolah naungan
  #2.1 read table sekolah naungan
  read_tables = pd.read_html(raw)
  sekolah_naungan_table = read_tables[index_table_sekolah]

  #2.2 link sekolah naungan
  npsn_url_tags = soup.find('table', id='tabelsekolah').find('tbody').find_all('a', href=True)
  if npsn_url_tags:
    npsn_urls = [tag['href'] for tag in npsn_url_tags]
    url_dict = {'url': npsn_urls}
  else:
    url_dict = {'url': None}
  sekolah_naungan_table = sekolah_naungan_table.assign(**url_dict)

  #3. parsing profile yayasan
  #3.1 bikin dictionary kosong for each profile
  profile_yayasan_dict = make_profile_yayasan_dict()

  #3.2 header yayasan
  header = soup.find('h4', class_='page-header').get_text().strip() #get all text in h4
  address = soup.find('font', class_='small').get_text().strip() #get address only
  yayasan = header.split(f' {address}')[0]
  yayasan = yayasan.split(' ', 1)

  nama_yayasan = yayasan[1]
  kode_yayasan = yayasan[0][1:len(yayasan[0])-1]

  #3.3 profile yayasan
  li_profil_yayasan = soup.find('ul', class_='list-group').find_all('li')
  profile_yayasan = [li.get_text().split(' : ')[1] for li in li_profil_yayasan]
  profile_yayasan.append(nama_yayasan)
  profile_yayasan.append(kode_yayasan)
  for i, (key, value) in enumerate(profile_yayasan_dict.items()):
    value.append(profile_yayasan[i])

  #4. make repeated profile yayasan to be inserted to sekolah_naungan_table
  repeated_profile = {key: value * len(sekolah_naungan_table) for key, value in profile_yayasan_dict.items()}
  profile_and_schools = sekolah_naungan_table.assign(**repeated_profile)
  #5. append dataframes
  # exec(f'profile_yayasan_list{i+1}.append(profile_and_schools)')
  profile_yayasan_list.append(profile_and_schools)
  return None

def parse_cabang_yayasan(soup):
  npyp_url_tags = soup.find('table', id='tabelyayasan').find('tbody').find_all('a', href=True)
  if npyp_url_tags:
    for tag in npyp_url_tags:
      additional_npyp_urls.append(tag['href'])
  return None

def parse_page(url, driver, profile_yayasan_list, max_retries=3):
  retry_count=0
  while retry_count < max_retries:
    try:
      driver.get(url)
      content_flag = 0
      sekolah_flag = 0
      yayasan_flag = 0
      try:
        check_content = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.CLASS_NAME, 'box')))
        content_flag = 1
      except (TimeoutException, NoSuchElementException):
        pass
      
      try:
        select_sekolah = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.NAME, 'tabelsekolah_length')))
        sekolah_flag = 1
      except (TimeoutException, NoSuchElementException):
        pass
      
      try:
        select_yayasan = WebDriverWait(driver, timeout_limit).until(EC.presence_of_element_located((By.NAME, 'tabelyayasan_length')))
        yayasan_flag = 1
      except (TimeoutException, NoSuchElementException):
        retry_count = 2
      
      index_table_sekolah = 0
      if content_flag == 1:
        if sekolah_flag == 1 and yayasan_flag == 0:
          #1. cari dropdown filter untuk set select = 'all'
          dropdown_sekolah = Select(select_sekolah)
          dropdown_sekolah.select_by_value('-1')

          raw = driver.page_source
          soup = BeautifulSoup(raw, 'html.parser') # untuk parsing
          parse_profile_and_sekolah(raw, soup, profile_yayasan_list, index_table_sekolah)
          break

        elif yayasan_flag == 0 and sekolah_flag == 1:
          #1. cari dropdown filter untuk set select = 'all'
          dropdown_yayasan = Select(select_yayasan)
          dropdown_yayasan.select_by_value('-1')

          raw = driver.page_source
          soup = BeautifulSoup(raw, 'html.parser') # untuk parsing
          parse_cabang_yayasan(soup)

        if sekolah_flag == 1 and yayasan_flag == 1:
          index_table_sekolah = 1
          #1. cari dropdown filter untuk set select = 'all'
          dropdown_sekolah = Select(select_sekolah)
          dropdown_sekolah.select_by_value('-1')

          dropdown_yayasan = Select(select_yayasan)
          dropdown_yayasan.select_by_value('-1')
          
          raw = driver.page_source
          soup = BeautifulSoup(raw, 'html.parser') # untuk parsing
          parse_cabang_yayasan(soup)
          parse_profile_and_sekolah(raw, soup, profile_yayasan_list, index_table_sekolah)
          break

        else:
          print(f'table sekolah unavailable at try: {retry_count}, {url[-36:]}')
          retry_count += 1
          time.sleep(2)
          continue

      else:
        print(f'content unavailable at try: {retry_count}, {url[-36:]}')
        retry_count += 1
        time.sleep(2)
        continue

    except Exception as e:
      # Handle the exception here without stopping the script
      print(f'exception error     at try: {retry_count}, {url[-36:]},  {e}')
      if retry_count == 2:
        failed_yayasan.append(url)
      retry_count += 1
      time.sleep(2)
      continue
  return None

def parse_pages(urls, yayasan_profiles):
  driver = driversetup()
  for url in urls:
    parse_page(url, driver, yayasan_profiles)
  driver.quit()
  return None

def main_yayasan(yayasan_partition, yayasan_profiles):
  threads = []
  num_threads = cores
  yayasan_batches = np.array_split(yayasan_partition, num_threads)
  for t in range(num_threads):
    thread = threading.Thread(target=parse_pages, args=(yayasan_batches[t], yayasan_profiles))
    threads.append(thread)

  for thread in threads:
    thread.start()

  for thread in threads:
    thread.join()

  return None

partition = 4
yayasan_partition = np.array_split(df_yayasan, partition)

yayasan_profiles_1 = []
yayasan_profiles_2 = []
yayasan_profiles_3 = []
yayasan_profiles_4 = []

additional_yayasan_profiles = []

additional_npyp_urls = []
failed_yayasan = [] #yayasan yang gagal di scrape

# jalanin scraping per partisi
process_prof_yayasan_time = time.time()
main_yayasan(yayasan_partition[0], yayasan_profiles_1)
main_yayasan(yayasan_partition[1], yayasan_profiles_2)
main_yayasan(yayasan_partition[2], yayasan_profiles_3)
main_yayasan(yayasan_partition[3], yayasan_profiles_4)


flag = 0
while additional_npyp_urls:
  if flag == 2:
    print('previous additional npyp still cannot be proccessed after third retry:')
    for i in additional_npyp_urls:
      print(i)
    break
  additional_npyp_urls = [*set(additional_npyp_urls)]
  temp_additional_npyp_urls = additional_npyp_urls[:]
  main_yayasan(additional_npyp_urls, additional_yayasan_profiles)
  if temp_additional_npyp_urls != additional_npyp_urls:
    additional_npyp_urls = additional_npyp_urls[len(temp_additional_npyp_urls):]
    flag = 0
  else:
    flag += 1

yayasan_profiles = yayasan_profiles_1 + yayasan_profiles_2 + yayasan_profiles_3 + yayasan_profiles_4 + additional_yayasan_profiles
yayasan_profiles = pd.concat(yayasan_profiles, ignore_index=True)
yayasan_profiles.to_csv(f'./dataset/yayasan_profiles_{current_date}.csv', index=False)

print(f'get yayasan profiles done in: {time.time() - start_yayasan_profiles}')