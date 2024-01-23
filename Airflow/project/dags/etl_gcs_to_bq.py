import os
import logging
import pandas as pd
import requests
import pytz
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from google.cloud import storage
from airflow.utils.task_group import TaskGroup


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
tipe_data = ".parquet"
path_to_raw_dataset = '/opt/airflow/dataset/raw'
path_to_transform_dataset = '/opt/airflow/dataset/transform'


def extract_data_jakarta_2014():
    url_prefix = 'https://data.jakarta.go.id/dataset/261f29c7a1503f0b753b985a56fecffc/resource'
    
    list_url = [f'{url_prefix}/23fa548d468d650bc8bd5058fec784aa/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2014.csv',
                f'{url_prefix}/87f6175eb5f3b37425949f20260e9d72/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2014.csv',
                f'{url_prefix}/523070ee388e02e4358f8c5f196e10de/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2014.csv',
                f'{url_prefix}/241a200e7d9d11c8579946b065d622a8/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2014.csv',
                f'{url_prefix}/ae71fc78d527c5cf6b76d168fc8b6beb/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2014.csv',
                f'{url_prefix}/062bf86efc98efa63897ae2eb6c80479/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2014.csv',
                f'{url_prefix}/af9921320cab5766318573f988493d31/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2014.csv',
                f'{url_prefix}/d806cfe06338dec39f4c9ff299b51770/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2014.csv',
                f'{url_prefix}/e6c81f16fdc541b8a9844546ae22e2a4/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2014.csv',
                f'{url_prefix}/619499be0733fc44ced2f8a70fdcb010/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2014.csv',
                f'{url_prefix}/7f23854b07c31669bf794b38c115ccfe/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2014.csv',
                f'{url_prefix}/897422a5babfaf21bd6e9bb5b3535fa2/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2014.csv',
                 ]
    
    def download_file(url, folder_path):
        filename = url.split('/')[-1]
        file_path = os.path.join(folder_path, filename)
        response = requests.get(url)

        # Membuka file dengan mode 'write binary' di folder yang ditentukan
        with open(file_path, 'wb') as file:
            file.write(response.content)

    folder_path = path_to_raw_dataset
    for url in list_url:
        download_file(url, folder_path)


def extract_data_jakarta_2015():
    url_prefix = 'https://data.jakarta.go.id/dataset/8091a3655ef6a834432fb6b6501b9e94/resource'

    list_url = [f'{url_prefix}/e4e551b06873e264f82c2fc781ba50c8/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2015.csv',
                f'{url_prefix}/7ba050c8b3616a12969817d4af6a94bd/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2015.csv',
                f'{url_prefix}/750d77309766c104d20645c97c45344c/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2015.csv',
                f'{url_prefix}/e2a10a0deb93fb3c2ea76bcd2d14b82c/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2015.csv',
                f'{url_prefix}/6aa890ab63934df8f6e758088b64bc3f/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2015.csv',
                f'{url_prefix}/bf183c83da6496314c9b582a3237f5bf/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2015.csv',
                f'{url_prefix}/bef876dc24a8b51d5585057f079f89ef/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2015.csv',
                f'{url_prefix}/4f3ab9fb5a6b609b5a13a30ea022254f/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2015.csv',
                f'{url_prefix}/7839066084ccfe511169e19257a3afe4/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2015.csv',
                f'{url_prefix}/a938016c592a7351f31c96032d566ed6/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2015.csv',
                f'{url_prefix}/51e373fb34f98ac5d7dab87f31373099/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2015.csv',
                f'{url_prefix}/b60a8320f4b7c5b53c5a83c39821bde3/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2015.csv',
                 ]
    
    def download_file(url, folder_path):
        filename = url.split('/')[-1]
        file_path = os.path.join(folder_path, filename)
        response = requests.get(url)

        # Membuka file dengan mode 'write binary' di folder yang ditentukan
        with open(file_path, 'wb') as file:
            file.write(response.content)

    folder_path = path_to_raw_dataset
    for url in list_url:
        download_file(url, folder_path)


def extract_data_jakarta_2016():
    url_prefix = 'https://data.jakarta.go.id/dataset/2760a0c2076f906e7dae3ff66ab18bb6/resource'

    list_url = [f'{url_prefix}/3934386f484faffff4a1f1e76fc0d4b9/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2016.csv',
                f'{url_prefix}/18dbfbb564378e1323bce163e2044d76/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2016.csv',
                f'{url_prefix}/6244d016a77e7dd53931df83bec8a71d/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2016.csv',
                f'{url_prefix}/0d90ec62e422311a4d4a785f4f46d0b6/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2016.csv',
                f'{url_prefix}/650aed01361b553d0dd900fba0b9d6ba/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2016.csv',
                f'{url_prefix}/0b1e6eca4d6f8653e1bbc2ebd1162755/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2016.csv',
                f'{url_prefix}/91fcd752846949e517f8dd528b805020/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2016.csv',
                f'{url_prefix}/3f0200c0858e2c3f0621702f6d282dc6/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2016.csv',
                f'{url_prefix}/bf27eb0d3eccb57dec8dd1983803a3ef/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2016.csv',
                f'{url_prefix}/5ec64e33cbb639a9389fee702206727b/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2016.csv',
                f'{url_prefix}/f03fd10a48f06a821de4db4026d03ab5/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2016.csv',
                f'{url_prefix}/a442253bd2f0c76d798452fb3bc798ea/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2016.csv',
                 ]
    
    def download_file(url, folder_path):
        filename = url.split('/')[-1]
        file_path = os.path.join(folder_path, filename)
        response = requests.get(url)

        # Membuka file dengan mode 'write binary' di folder yang ditentukan
        with open(file_path, 'wb') as file:
            file.write(response.content)

    folder_path = path_to_raw_dataset
    for url in list_url:
        download_file(url, folder_path)


def extract_data_jakarta_2021():
    url_prefix = 'https://data.jakarta.go.id/dataset/3b56c00404adf8feb3f667cbbd700b7c/resource'

    list_url = [f'{url_prefix}/c710303c60631a904b4f0896a842c685/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2021.csv',
                f'{url_prefix}/fd7baa5eb1fe779e2d43f7ce1ff88ee3/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2021.csv',
                f'{url_prefix}/b93b0a07d5f4ba9cad459748d0ed06c0/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2021.csv',
                f'{url_prefix}/225e6e838312eed24d951f0fca58596e/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2021.csv',
                f'{url_prefix}/2e39ba6178a132a381ec073b94c9ede6/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2021.csv',
                f'{url_prefix}/cd2399dab95d976c11c42d7909aba0eb/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2021.csv',
                f'{url_prefix}/2c22721cfcd2f932e8bb7a0e2a22e82a/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2021.csv',
                f'{url_prefix}/ae98bb60b96c74627ece0c274186043d/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2021.csv',
                f'{url_prefix}/79aac0c6384472d1c3687f5a968e74ab/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2021.csv',
                f'{url_prefix}/3777775ea4652ce8b140ac56f75c912e/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2021.csv',
                f'{url_prefix}/f06cd7ab4925d3ac6c800a46101a130d/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2021.csv',
                f'{url_prefix}/ae010f6aff7eb4e6b74a6bb35a2439ad/download/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2021.csv',
                 ]
    
    def download_file(url, folder_path):
        filename = url.split('/')[-1]
        file_path = os.path.join(folder_path, filename)
        response = requests.get(url)

        # Membuka file dengan mode 'write binary' di folder yang ditentukan
        with open(file_path, 'wb') as file:
            file.write(response.content)

    folder_path = path_to_raw_dataset
    for url in list_url:
        download_file(url, folder_path)



# Transform
def transform_data_jakarta_2014():
    list_data = [f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2014.csv'
                 ]
    dfs = []
    for data in range(len(list_data)):
        data = list_data[data]
        header = ['tanggal','stasiun','pm10','so2','co','o3','no2','max','critical','categori']
        df = pd.read_csv(data, header=None, skiprows=1, names=header)
        df.columns = df.columns.str.lower()
        df.dropna(inplace=True)
        df = df.replace('---', pd.NA)
        df.dropna(inplace=True)
        
        df = df.astype({'pm10':int, 'so2':int, 'co':int, 'o3':int, 'no2':int, 'max':int, 'tanggal':str})
        df.drop_duplicates(inplace=True)
        df['tanggal'] = pd.to_datetime(df['tanggal'])
        dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True)
    result_df.insert(0, 'id', range(1, result_df.shape[0] + 1))
    result_df = result_df.astype({'id':str})
    save = result_df.to_parquet(f'{path_to_transform_dataset}/data_polusi_jakarta_2014{tipe_data}', index=False)
    return save


def transform_data_jakarta_2015():
    list_data = [f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2015.csv'
                 ]
    dfs = []
    for data in range(len(list_data)):
        data = list_data[data]
        header = ['tanggal','stasiun','pm10','so2','co','o3','no2','max','critical','categori']
        df = pd.read_csv(data, header=None, skiprows=1, names=header)
        df.columns = df.columns.str.lower()
        df.dropna(inplace=True)
        df = df.replace('---', pd.NA)
        df.dropna(inplace=True)
        
        df = df.astype({'pm10':int, 'so2':int, 'co':int, 'o3':int, 'no2':int, 'max':int, 'tanggal':str})
        df.drop_duplicates(inplace=True)
        df['tanggal'] = pd.to_datetime(df['tanggal'])
        dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True)
    result_df.insert(0, 'id', range(1, result_df.shape[0] + 1))
    result_df = result_df.astype({'id':str})
    save = result_df.to_parquet(f'{path_to_transform_dataset}/data_polusi_jakarta_2015{tipe_data}', index=False)
    return save


def transform_data_jakarta_2016():
    list_data = [f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2016.csv'
                 ]
    dfs = []
    for data in range(len(list_data)):
        data = list_data[data]
        header = ['tanggal','stasiun','pm10','so2','co','o3','no2','max','critical','categori']
        df = pd.read_csv(data, header=None, skiprows=1, names=header)
        df.columns = df.columns.str.lower()
        df.dropna(inplace=True)
        df = df.replace('---', pd.NA)
        df.dropna(inplace=True)
        
        df = df.astype({'pm10':int, 'so2':int, 'co':int, 'o3':int, 'no2':int, 'max':int, 'tanggal':str})
        df.drop_duplicates(inplace=True)
        # df['tanggal'] = df['tanggal'].str.replace('/', '-')
        df['tanggal'] = pd.to_datetime(df['tanggal'], errors='coerce')
        df.dropna(inplace=True)
        dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True)
    result_df.insert(0, 'id', range(1, result_df.shape[0] + 1))
    result_df = result_df.astype({'id':str})
    save = result_df.to_parquet(f'{path_to_transform_dataset}/data_polusi_jakarta_2016{tipe_data}', index=False)
    return save


def transform_data_jakarta_2021():
    list_data = [f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2021.csv'
                 ]
    dfs = []
    for data in range(len(list_data)):
        data = list_data[data]
        header = ['tanggal','stasiun','pm10','pm25', 'so2','co','o3','no2','max','critical','categori']
        df = pd.read_csv(data, header=None, skiprows=1, names=header)
        df.columns = df.columns.str.lower()
        df.dropna(inplace=True)
        df = df.replace('---', pd.NA)
        df.dropna(inplace=True)
        
        df = df.astype({'pm10':int, 'pm25':int, 'so2':int, 'co':int, 'o3':int, 'no2':int, 'max':int, 'tanggal':str})
        df.drop_duplicates(inplace=True)
        # df['tanggal'] = df['tanggal'].str.replace('/', '-')
        df['tanggal'] = pd.to_datetime(df['tanggal'])
        dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True)
    result_df.insert(0, 'id', range(1, result_df.shape[0] + 1))
    result_df = result_df.astype({'id':str})
    save = result_df.to_parquet(f'{path_to_transform_dataset}/data_polusi_jakarta_2021{tipe_data}', index=False)
    return save

# merge data polusi jakarta 2014 - 2016
def merge_data_jakarta_2015_sd_2016():
    dfs = []
    list_data = [
        f'{path_to_transform_dataset}/data_polusi_jakarta_2014{tipe_data}',
        f'{path_to_transform_dataset}/data_polusi_jakarta_2015{tipe_data}',
        f'{path_to_transform_dataset}/data_polusi_jakarta_2016{tipe_data}',
    ]

    for data in range(len(list_data)):
        data = list_data[data]
        df = pd.read_parquet(data)
        dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True)
    save = result_df.to_parquet(f'{path_to_transform_dataset}/data_polusi_jakarta_2014_sd_2016{tipe_data}', index=False)
    return save



# Load
def upload_raw_2014_to_gcs(**kwargs):
    list_data = [f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2014.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2014.csv'
                 ]
    for file_path in list_data:
        filename = file_path.split('/')[-1]
        dst_path = f"raw/{filename}"
        
        # Buat task LocalFilesystemToGCSOperator
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{filename}_to_gcs',
            src=file_path,
            dst=dst_path,
            bucket=BUCKET,
        )
        upload_task.execute(context=kwargs)

def upload_raw_2015_to_gcs(**kwargs):
    list_data = [f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2015.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2015.csv'
                 ]
    for file_path in list_data:
        filename = file_path.split('/')[-1]
        dst_path = f"raw/{filename}"
        
        # Buat task LocalFilesystemToGCSOperator
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{filename}_to_gcs',
            src=file_path,
            dst=dst_path,
            bucket=BUCKET,
        )
        upload_task.execute(context=kwargs)

def upload_raw_2016_to_gcs(**kwargs):
    list_data = [f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2016.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2016.csv'
                 ]
    for file_path in list_data:
        filename = file_path.split('/')[-1]
        dst_path = f"raw/{filename}"
        
        # Buat task LocalFilesystemToGCSOperator
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{filename}_to_gcs',
            src=file_path,
            dst=dst_path,
            bucket=BUCKET,
        )
        upload_task.execute(context=kwargs)

def upload_raw_2021_to_gcs(**kwargs):
    list_data = [f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Januari-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Februari-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Maret-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-April-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Mei-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juni-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Juli-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Agustus-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-September-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Oktober-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-November-Tahun-2021.csv',
                 f'{path_to_raw_dataset}/Indeks-Standar-Pencemar-Udara-di-SPKU-Bulan-Desember-Tahun-2021.csv'
                 ]
    for file_path in list_data:
        filename = file_path.split('/')[-1]
        dst_path = f"raw/{filename}"
        
        # Buat task LocalFilesystemToGCSOperator
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{filename}_to_gcs',
            src=file_path,
            dst=dst_path,
            bucket=BUCKET,
        )
        upload_task.execute(context=kwargs)


with DAG(
    dag_id="etl_local_to_gcs_to_bigquery",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    with TaskGroup('extract_data') as extract_data:
        extract_raw_data_2014 = PythonOperator(
            task_id = 'extract_raw_data_2014',
            python_callable = extract_data_jakarta_2014
        )
        extract_raw_data_2015 = PythonOperator(
            task_id = 'extract_raw_data_2015',
            python_callable = extract_data_jakarta_2015
        )
        extract_raw_data_2016 = PythonOperator(
            task_id = 'extract_raw_data_2016',
            python_callable = extract_data_jakarta_2016
        )
        extract_raw_data_2021 = PythonOperator(
            task_id = 'extract_raw_data_2021',
            python_callable = extract_data_jakarta_2021
        )

        extract_raw_data_2014 >> extract_raw_data_2015 >> \
        extract_raw_data_2016 >> extract_raw_data_2021

    with TaskGroup('transforms_data') as transforms_data:
        transform_raw_data_2014 = PythonOperator(
            task_id = 'transform_raw_data_2014',
            python_callable = transform_data_jakarta_2014
        )
        transform_raw_data_2015 = PythonOperator(
            task_id = 'transform_raw_data_2015',
            python_callable = transform_data_jakarta_2015
        )
        transform_raw_data_2016 = PythonOperator(
            task_id = 'transform_raw_data_2016',
            python_callable = transform_data_jakarta_2016
        )
        transform_raw_data_2021 = PythonOperator(
            task_id = 'transform_raw_data_2021',
            python_callable = transform_data_jakarta_2021
        )
        merge_data = PythonOperator(
            task_id = 'merge_data',
            python_callable = merge_data_jakarta_2015_sd_2016
        )

        transform_raw_data_2014 >> transform_raw_data_2015 >> \
        transform_raw_data_2016 >> transform_raw_data_2021 >> \
        merge_data

    with TaskGroup('loads_raw_data_to_gcs') as loads_raw_data_to_gcs:
        upload_raw_data_2014_to_gcs = PythonOperator(
                task_id = 'upload_raw_data_2014_to_gcs',
                python_callable=upload_raw_2014_to_gcs,
            )
        upload_raw_data_2015_to_gcs = PythonOperator(
                task_id = 'upload_raw_data_2015_to_gcs',
                python_callable=upload_raw_2015_to_gcs,
            )
        upload_raw_data_2016_to_gcs = PythonOperator(
                task_id = 'upload_raw_data_2016_to_gcs',
                python_callable=upload_raw_2016_to_gcs,
            )
        upload_raw_data_2021_to_gcs = PythonOperator(
                task_id = 'upload_raw_data_2021_to_gcs',
                python_callable=upload_raw_2021_to_gcs,
            )
        
        upload_raw_data_2014_to_gcs >> upload_raw_data_2015_to_gcs >> \
        upload_raw_data_2016_to_gcs >> upload_raw_data_2021_to_gcs
    
    with TaskGroup('loads_transform_data_to_gcs') as loads_transform_data_to_gcs:
        upload_transform_data_2014_to_gcs = LocalFilesystemToGCSOperator(
                task_id = 'upload_transform_data_2014_to_gcs',
                src = f"{path_to_transform_dataset}/data_polusi_jakarta_2014{tipe_data}",
                dst = f"transform/data_polusi_jakarta_2014{tipe_data}",
                bucket = BUCKET
            )
        upload_transform_data_2015_to_gcs = LocalFilesystemToGCSOperator(
                task_id = 'upload_transform_data_2015_to_gcs',
                src = f"{path_to_transform_dataset}/data_polusi_jakarta_2015{tipe_data}",
                dst = f"transform/data_polusi_jakarta_2015{tipe_data}",
                bucket = BUCKET
            )
        upload_transform_data_2016_to_gcs = LocalFilesystemToGCSOperator(
                task_id = 'upload_transform_data_2016_to_gcs',
                src = f"{path_to_transform_dataset}/data_polusi_jakarta_2016{tipe_data}",
                dst = f"transform/data_polusi_jakarta_2016{tipe_data}",
                bucket = BUCKET
            )
        upload_transform_data_2021_to_gcs = LocalFilesystemToGCSOperator(
                task_id = 'upload_transform_data_2021_to_gcs',
                src = f"{path_to_transform_dataset}/data_polusi_jakarta_2021{tipe_data}",
                dst = f"transform/data_polusi_jakarta_2021{tipe_data}",
                bucket = BUCKET
            )
        upload_merge_data_to_gcs = LocalFilesystemToGCSOperator(
                task_id = 'upload_merge_data_to_gcs',
                src = f"{path_to_transform_dataset}/data_polusi_jakarta_2014_sd_2016{tipe_data}",
                dst = f"transform/data_polusi_jakarta_2014_sd_2016{tipe_data}",
                bucket = BUCKET
            )
        upload_transform_data_2014_to_gcs >> upload_transform_data_2015_to_gcs >> \
        upload_transform_data_2016_to_gcs >> upload_transform_data_2021_to_gcs >> \
        upload_merge_data_to_gcs


    with TaskGroup('load_transform_data_gcs_to_bigquery') as load_transform_data_gcs_to_bigquery:
        load_transform_data_jakarta_2014_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_transform_data_jakarta_2014_to_bigquery',
            bucket = BUCKET,
            source_format ="PARQUET",
            source_objects = f"transform/data_polusi_jakarta_2014{tipe_data}",
            destination_project_dataset_table = PROJECT_ID + ".ampun_puh_sepuh.data_polusi_jakarta_2014",
            write_disposition = 'WRITE_TRUNCATE',
        )
        load_transform_data_jakarta_2015_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_transform_data_jakarta_2015_to_bigquery',
            bucket = BUCKET,
            source_format ="PARQUET",
            source_objects = f"transform/data_polusi_jakarta_2015{tipe_data}",
            destination_project_dataset_table = PROJECT_ID + ".ampun_puh_sepuh.data_polusi_jakarta_2015",
            write_disposition = 'WRITE_TRUNCATE',
        )
        load_transform_data_jakarta_2016_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_transform_data_jakarta_2016_to_bigquery',
            bucket = BUCKET,
            source_format ="PARQUET",
            source_objects = f"transform/data_polusi_jakarta_2016{tipe_data}",
            destination_project_dataset_table = PROJECT_ID + ".ampun_puh_sepuh.data_polusi_jakarta_2016",
            write_disposition = 'WRITE_TRUNCATE',
        )
        load_transform_data_jakarta_2021_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_transform_data_jakarta_2021_to_bigquery',
            bucket = BUCKET,
            source_format ="PARQUET",
            source_objects = f"transform/data_polusi_jakarta_2021{tipe_data}",
            destination_project_dataset_table = PROJECT_ID + ".ampun_puh_sepuh.data_polusi_jakarta_2021",
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_TRUNCATE',
        )
        load_merge_data_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_merge_data_to_bigquery',
            bucket = BUCKET,
            source_format ="PARQUET",
            source_objects = f"transform/data_polusi_jakarta_2014_sd_2016{tipe_data}",
            destination_project_dataset_table = PROJECT_ID + ".ampun_puh_sepuh.data_polusi_jakarta_2014_sd_2016",
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_TRUNCATE',
        )
        load_transform_data_jakarta_2014_to_bigquery >> load_transform_data_jakarta_2015_to_bigquery >> \
        load_transform_data_jakarta_2016_to_bigquery >> load_transform_data_jakarta_2021_to_bigquery >> \
        load_merge_data_to_bigquery
    
    
    extract_data >> transforms_data >> loads_raw_data_to_gcs >> loads_transform_data_to_gcs >> load_transform_data_gcs_to_bigquery