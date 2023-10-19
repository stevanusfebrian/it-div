import pandas as pd

"""COMBINE result-dataset"""
# file ini dipakai untuk menggabungkan beberapa pecahan
# data yang sudah di scrape dari folder dataset
result_profile_yayasan = pd.concat(map(pd.read_csv, [
    './dataset/df_yayasan_0_to_7000.csv',
    './dataset/df_yayasan_7000_to_12000.csv',
    './dataset/df_yayasan_12000_to_20000.csv',
    './dataset/df_yayasan_20000_to_25000.csv',
    './dataset/df_yayasan_25000_to_30000.csv',
    './dataset/df_yayasan_30000_to_35000.csv',
    './dataset/df_yayasan_35000_to_43000.csv',
    './dataset/df_yayasan_43000_to_51000.csv',
    './dataset/df_yayasan_51000_to_60000.csv',
    './dataset/df_yayasan_60000_to_65000.csv',
    './dataset/df_yayasan_65000_to_75000.csv',
    './dataset/df_yayasan_75000_to_80000.csv',
    './dataset/df_yayasan_80000_to_100000.csv',
    './dataset/df_yayasan_100000_to_120000.csv',
    './dataset/df_yayasan_120000_to_130000.csv',
    './dataset/df_yayasan_130000_to_end.csv'
]))

result_profile_yayasan.to_csv('./result-dataset/yayasan_profiles_result.csv', index=False)