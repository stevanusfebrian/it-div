# -*- coding: utf-8 -*-
import os
import platform
import time
import pandas as pd
import numpy as np
import threading
from bs4 import BeautifulSoup

# for logging
import sys
import logging
import datetime
from logging.handlers import TimedRotatingFileHandler

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

#logging
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

class PrintLogger:
  def __init__(self, log):
    self.terminal = sys.stdout
    self.log = log

  def write(self, message):
    self.terminal.write(message)
    self.log.write(message)

  def flush(self):
    pass

def setup_logging():
  log_formatter = logging.Formatter("%(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S")
  log_file = f'./loggings/dapodik_profile_schools_{current_date}.log'
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
cores = os.cpu_count()
print(f'CPU CORES:    {cores}') 

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
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

def driversetup():
  chrome_service = Service(executable_path=chrome_path)
  driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
  return driver

#profile user sekolah
profile_user_dict = {
    'kepsek': [],
    'operator': [],
    'akreditasi': [],
    'kurikulum': [],
    'waktu': [],
    'nama_sekolah': [],
    'sekolah_id': []
}

# identitas sekolah
identitas_sekolah_dict = {
    'npsn': [],
    'status': [],
    'bentuk_pendidikan': [],
    'status_kepemilikan': [],
    'sk_pendirian_sekolah': [],
    'tanggal_sk_pendirian': [],
    'sk_izin_operasional': [],
    'tanggal_sk_izin_operasional': []
}

#data pelengkap
data_pelengkap_dict = {
    'kebutuhan_khusus': [],
    'nama_bank': [],
    'kcp': [],
    'nama_di_rek': []
}

#data rinci
data_rinci_dict = {
    'status_bos': [],
    'waku_penyelenggaraan': [],
    'sertifikasi_iso': [],
    'sumber_listrik': [],
    'daya_listri': [],
    'akses_internet': []
}

#halaman tab kontak
kontak_utama_dict = {
    'alamat': [],
    'dusun': [],
    'desa_kelurahan': [],
    'kecamatan': [],
    'kabupaten': [],
    'provinsi': [],
    'kode_pos': [],
    'lintang': [],
    'bujur': [],
    'rt': [],
    'rw': []
}

#create empty dataframe

kode_wilayah_kabupaten = pd.Series([])
kode_wilayah_kecamatan = pd.Series([])
sekolah_id_enkrip = pd.Series([])


"""## get kode wilayah `provinsi`"""
start_provinsi = time.time()
print('getting provinsi')
province_api = 'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=0&kode_wilayah=000000&semester_id=20221'
provinces = pd.read_json(province_api)

provinces = provinces.sort_values('kode_wilayah', ignore_index=True)
provinces = provinces['kode_wilayah']
provinces = provinces.astype(str).str.zfill(6)
print(f'get provinsi done in: {time.time() - start_provinsi} seconds')
print('provinces', len(provinces))


"""## get kode wilayah `kabupaten`"""
start_kab_kota = time.time()
print('getting kabupaten')
for province in provinces:
    try:
      province = province.strip()
      kab_api = f'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=1&kode_wilayah={province}&semester_id=20221'
      kabupatens = pd.read_json(kab_api)
      kabupatens = kabupatens.sort_values('kode_wilayah')
      kabupatens = kabupatens['kode_wilayah']
      kode_wilayah_kabupaten = kode_wilayah_kabupaten._append(kabupatens, ignore_index=True)
    except Exception as e:
      print(f'get kabupaten error: {e}')

kode_wilayah_kabupaten = kode_wilayah_kabupaten.astype(str).str.zfill(6)
df_kode_wilayah_kabupaten = pd.DataFrame({'kode_wilayah_kabupaten': kode_wilayah_kabupaten})
df_kode_wilayah_kabupaten.to_csv('./dataset/kode_wilayah_kabupaten.csv', index=False)
print('kode_wilayah_kabupaten: ', len(kode_wilayah_kabupaten))
print(f'get kab/kota done in: {time.time() - start_kab_kota} seconds')


"""## get kode wilayah `Kecamatan`"""
start_kecamatan = time.time()
print('getting kecamatan')
for kabupaten in kode_wilayah_kabupaten:
    try:
      kabupaten = kabupaten.strip()
      kec_api = f'https://dapo.kemdikbud.go.id/rekap/dataPD?id_level_wilayah=2&kode_wilayah={kabupaten}&semester_id=20221'
      kecamatans = pd.read_json(kec_api)
      kecamatans = kecamatans.sort_values('kode_wilayah')
      kecamatans = kecamatans['kode_wilayah']
      kode_wilayah_kecamatan = kode_wilayah_kecamatan._append(kecamatans, ignore_index=True)
    except Exception as e:
      print(f'get kecamatan error: {e}')

kode_wilayah_kecamatan = kode_wilayah_kecamatan.astype(str).str.zfill(6)
df_kode_wilayah_kecamatan = pd.DataFrame({'kode_wilayah_kecamatan': kode_wilayah_kecamatan})
df_kode_wilayah_kecamatan.to_csv('./dataset/kode_wilayah_kecamatan.csv', index=False)
print(f'get kecamatan done in: {time.time() - start_kecamatan} seconds')
print('kode_wilayah_kecamatan: ', len(kode_wilayah_kecamatan))

"""## get ID sekolah and School Name"""
start_sekolah = time.time()
df_sekolah_id_nama = pd.DataFrame()
print('getting sekolah urls')
for kecamatan in kode_wilayah_kecamatan:
  kecamatan = kecamatan.strip()
  school_list_api = f'https://dapo.kemdikbud.go.id/rekap/progresSP?id_level_wilayah=3&kode_wilayah={kecamatan}&semester_id=20221'
  school_list = pd.read_json(school_list_api)
  sekolah_id_nama = school_list[['sekolah_id_enkrip', 'nama']].applymap(lambda x: x.strip() if isinstance(x, str) else x)
  df_sekolah_id_nama = pd.concat([df_sekolah_id_nama, sekolah_id_nama], axis=0, ignore_index=True)


"""## get Sekolah urls"""
sekolah_id_enkrip = df_sekolah_id_nama['sekolah_id_enkrip']
base_sekolah_url = 'https://dapo.kemdikbud.go.id/sekolah/'
sekolah_urls = [f'{base_sekolah_url}{sekolah_id}' for sekolah_id in sekolah_id_enkrip]

# save for progress
df_sekolah = pd.DataFrame({
   'nama_sekolah': df_sekolah_id_nama['nama'],
   'sekolah_id_enkrip': df_sekolah_id_nama['sekolah_id_enkrip'],
   'sekolah_urls': sekolah_urls
})

# df_sekolah_urls = pd.DataFrame({'sekolah_urls': sekolah_urls})
df_sekolah.to_csv('./dataset/sekolah_3_20221.csv', index=False)
print(f'get sekolah urls done in: {time.time() - start_sekolah} seconds')


"""## get profile sekolah"""
def append_data_to_dictionary(dictionary, parsed_list):
  for i, (key, value) in enumerate(dictionary.items()):
    value.append(parsed_list[i])
  return None

def parse_page(url, driver, max_retries=3):
  retry_count=0
  while retry_count < max_retries:
    try:
      driver.get(url)
      raw = driver.page_source
      soup = BeautifulSoup(raw, 'html.parser')
      if soup.find(class_='title', attrs={'data-content': '404 Error'}):
        print('404: ', url)
        return None

      #1. get "school id" from url and "school name"
      #1.1 school id
      school_id = url.split('/')[-1]

      #1.2 school name
      school_name = soup.find('h2', attrs={'class': 'name'}).get_text().strip()

      #2. get profile usermenu / content di kiri halaman
      profile_usermenu = soup.select_one('.profile-usermenu ul').find_all('li')
      profile_usermenu = [x.find('strong').get_text().strip() for x in profile_usermenu]

      #2.1 append School Id and Name to the profile_usermenuc
      profile_usermenu.append(school_name)
      profile_usermenu.append(school_id)

      #2.2 fill the actual dictionary with profile_usermenu dict value
      append_data_to_dictionary(profile_user_dict, profile_usermenu)

      #3. get profile cards container (Identitas Sekolah, Data Pelengkap, Data Rinci)
      profile_container = soup.find(id='profil')
      profile_cards = profile_container.find_all('div', attrs={'class': 'panel-info'})
      for card in profile_cards:
        [p.strong.decompose() for p in card.select('.panel-body p')] #remove strong tag

      for i, card in enumerate(profile_cards):
        if card.select_one('.panel-heading').get_text().strip() == 'Identitas Sekolah':
          identitas_sekolah = card.select_one('.panel-body').find_all('p') #cari kumpulan HTML p untuk Identitas Sekolah
          identitas_sekolah = [x.get_text().strip() for x in identitas_sekolah] #getting p values
          append_data_to_dictionary(identitas_sekolah_dict, identitas_sekolah) #sama kek 2.2
          continue

        elif card.select_one('.panel-heading').get_text().strip() == 'Data Pelengkap':
          data_pelengkap = card.select_one('.panel-body').find_all('p')
          data_pelengkap = [x.get_text().strip() for x in data_pelengkap]
          append_data_to_dictionary(data_pelengkap_dict, data_pelengkap)
          continue

        elif card.select_one('.panel-heading').get_text().strip() == 'Data Rinci':
          data_rinci = card.select_one('.panel-body').find_all('p')
          data_rinci = [x.get_text().strip() for x in data_rinci]
          append_data_to_dictionary(data_rinci_dict, data_rinci)
          continue

      #4. get kontak tab
      contact_container = soup.find(id='kontak')
      contact_cards = contact_container.find_all('div', attrs={'class': 'panel-info'})[0]
      contact_cards = contact_cards.select('.panel-body p')

      #4.1 Find RT & RW tag, parse the texts and values
      rt_rw = contact_cards[0].findNext('p')
      rt_rw_part, numbers_part = rt_rw.get_text().split(" : ")
      rt_value, rw_value = numbers_part.split(' / ')

      # 4.2 remove current strong tag
      [p.strong.decompose() for p in contact_cards]

      # 4.3 strip p tag
      contact_cards = [p.get_text().strip() for p in contact_cards]

      # 4.4 remove rt rw tag
      contact_cards.pop(1)

      # 4.5 append rt and rw value to the temp variable
      contact_cards.append(rt_value)
      contact_cards.append(rw_value)
      append_data_to_dictionary(kontak_utama_dict, contact_cards)
      return

    except Exception as e:
      print(f'{url}, error: {e}, retry: {retry_count}')
      if retry_count == 2:
        unprocessed_schools.append(url)
      retry_count += 1
      time.sleep(1)
  return None

def parse_pages(urls):
  driver = driversetup()
  for url in urls:
    parse_page(url, driver)
  driver.quit()
  return None

def main_school(school_partitions):
  threads = []
  num_threads = cores
  yayasan_batches = np.array_split(school_partitions, num_threads)
  for t in range(num_threads):
    thread = threading.Thread(target=parse_pages, args=(yayasan_batches[t],))
    threads.append(thread)

  for thread in threads:
    thread.start()

  for thread in threads:
    thread.join()
  return None

start_sekolah_profile = time.time()
sekolah_urls = pd.read_csv('./dataset/sekolah_url_v2.csv')
sekolah_urls = sekolah_urls['sekolah_urls']
print('sekolah_urls: ', len(sekolah_urls))
print('getting sekolah profile')

# Split the DataFrame into parts using pandas' np.array_split function
school_partitions = np.array_split(sekolah_urls, 100)
unprocessed_schools = []
for i in range(31, 32):
  main_school(school_partitions[i])

print('list of schools that fail to be scraped (excluding school with 404 error code)')
for url in unprocessed_schools:
  print(url);

merge_dict = {**profile_user_dict, **identitas_sekolah_dict, **data_pelengkap_dict, **data_rinci_dict, **kontak_utama_dict}

df_profile_dapodik_schools = pd.DataFrame(merge_dict)
df_profile_dapodik_schools = df_profile_dapodik_schools.drop_duplicates(subset=['sekolah_id'])
df_profile_dapodik_schools.to_csv(f'./scraped-data/school_dapodik_profiles_31_{current_date}.csv', index=False)
print(f'get profile sekolah done in: {time.time() - start_sekolah_profile} seconds')
