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
from airflow.utils.email import send_email


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
tipe_data = ".parquet"
path_to_raw_dataset = '/opt/airflow/dataset/raw'
path_to_transform_dataset = '/opt/airflow/dataset/transform'



# Transform
def transform_api():
    jakarta = requests.get('http://api.airvisual.com/v2/city?city=Jakarta&state=Jakarta&country=Indonesia&key=ff561aa8-460f-448b-abd2-005fb3496f23').json()
    aceh = requests.get('http://api.airvisual.com/v2/city?city=Banda%20Aceh&state=Aceh&country=Indonesia&key=ff561aa8-460f-448b-abd2-005fb3496f23').json()
    bali = requests.get('http://api.airvisual.com/v2/city?city=Denpasar&state=Bali&country=Indonesia&key=ff561aa8-460f-448b-abd2-005fb3496f23').json()
    tangsel = requests.get('http://api.airvisual.com/v2/city?city=South%20Tangerang&state=Banten&country=Indonesia&key=ff561aa8-460f-448b-abd2-005fb3496f23').json()
    semarang = requests.get('http://api.airvisual.com/v2/city?city=Semarang&state=Central%20Java&country=Indonesia&key=ff561aa8-460f-448b-abd2-005fb3496f23').json()
    
    list_data = [jakarta, aceh, bali, tangsel, semarang]

    dfs = []
    for data in range(len(list_data)):
        data = list_data[data]
        provinsi = data['data']['state']
        kota = data['data']['city']
        lat  = data['data']['location']['coordinates'][1]
        long = data['data']['location']['coordinates'][0]
        tanggal = data['data']['current']['pollution']['ts']
        aqius = data['data']['current']['pollution']['aqius']
        mainus = data['data']['current']['pollution']['mainus']

        # tanggal = datetime.strptime(tanggal, '%Y-%m-%dT%H:%M:%S.%fz')

        # # Mengonversi ke zona waktu Jakarta (WIB)
        # tanggal = tanggal.astimezone(pytz.timezone('Asia/Jakarta'))
        # tanggal = tanggal.date()

        data_dict = {
            'tanggal': [tanggal],
            'provinsi': [provinsi],
            'kota': [kota],
            'lat': [lat],
            'long': [long],
            'kualitas_udara': [aqius],
            'polutan_utama': [mainus]
        }

        df = pd.DataFrame.from_dict(data_dict)
        df['tanggal'] = pd.to_datetime(df['tanggal'])
        dfs.append(df)

    result_df = pd.concat(dfs, ignore_index=True)
    result_df.insert(0, 'id', range(1, result_df.shape[0] + 1))
    result_df = result_df.astype({'lat':str, 'long':str, 'id':str})
    
    def determine_category(value):
        if 0 <= value < 51:
            return 'Baik'
        elif 51 <= value < 101:
            return 'Sedang'
        elif value >= 101:
            return 'Buruk'
        else:
            return 'Kategori Tidak Ditemukan'

    # Terapkan fungsi dan membuat kolom 'kualitas_udara'
    result_df['kategori'] = result_df['kualitas_udara'].apply(determine_category)
    save = result_df.to_parquet(f"{path_to_transform_dataset}/api_data_polusi_indonesia{tipe_data}", index=False)
    return save


# Email Notification
def success_email_function(context):
    dag_run = context.get("dag_run")

    msg = "DAG run successfully"
    subject = f"DAG {dag_run} has completed"
    email = "driansptra21@gmail.com"
    send_email(to=email, subject=subject, html_content=msg)



with DAG(
    dag_id="etl_local_to_gcs_to_bigquery_api",
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    # on_success_callback=success_email_function,
    # default_args = {
    # "email": ["driansptra21@gmail.com", "massamunekun21@gmail.com"],
    # "email_on_failure": True,
    # "email_on_retry": False,
    # }
) as dag:
    
    transform_data = PythonOperator(
        task_id = 'extract_transform_raw_data',
        python_callable = transform_api
    )

    upload_transform_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_transform_data_to_gcs',
        src = f"{path_to_transform_dataset}/api_data_polusi_indonesia{tipe_data}",
        dst = f"transform/api_data_polusi_indonesia{tipe_data}",
        bucket = BUCKET
    )

    load_transform_data_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_transform_data_to_bigquery',
            bucket = BUCKET,
            source_format ="PARQUET",
            source_objects = f"transform/api_data_polusi_indonesia{tipe_data}",
            destination_project_dataset_table = PROJECT_ID + ".ampun_puh_sepuh.api_data_polusi_indonesia",
            create_disposition = 'CREATE_IF_NEEDED',
            write_disposition = 'WRITE_TRUNCATE',
    )

transform_data >> upload_transform_data_to_gcs >> load_transform_data_to_bigquery