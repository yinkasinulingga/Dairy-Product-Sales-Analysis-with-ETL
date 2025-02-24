'''
=================================================
Program ini bertujuan untuk mengotomatiskan proses ETL (Extract, Transform, Load) guna meningkatkan efisiensi operasional 
peternakan. Data produksi, penjualan, dan stok diekstrak dari PostgreSQL, dibersihkan dan ditransformasikan, lalu dimuat 
ke Elasticsearch untuk analisis lebih lanjut. Proses ini membantu mencegah kerugian, menjaga kualitas produk, serta 
mengoptimalkan strategi pemasaran dan distribusi.
=================================================
'''

# Import Libraries
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime

'''
Berikut merupakan fungsi dari library yang akan digunakan:
- Pandas digunakan untuk membuat, mengelola, dan memproses data dalam bentuk DataFrame.
- NumPy digunakan untuk membuat dan memanipulasi array, serta membantu dalam perhitungan numerik yang efisien.
- Airflow digunakan untuk mengotomatisasi proses Extract, Transform, Load (ETL), mulai dari ekstraksi data, transformasi, hingga pemuatan ke Elasticsearch.
- Elasticsearch digunakan sebagai tempat penyimpanan data pada API lokal, yang nantinya akan diakses dan divisualisasikan menggunakan Kibana.
- Datetime digunakan untuk menangani data waktu, seperti pencatatan waktu proses atau pengelolaan format tanggal
'''

# Default parameters untuk DAG
default_args = {
    'owner': 'inka',
    'retries': 0,  # tidak ingin ada retry
    'start_date': datetime(2024, 11, 1) 
}

# Fungsi Extract
def extract(**context):
    """
    Fungsi extract ini digunakan untuk mengambil / mengimport data dari database PostgreSQL dan menyimpannya dalam bentuk CSV agar bisa digunakan 
    pada tahap selanjutnya dalam proses ETL.
     
    Berikut merupakan tahapan yang dilakukan:
    1. Membuat koneksi ke database PostgreSQL menggunakan Airflow PostgresHook
    2. Mengekstrak data dari tabel 'table_m3'
    3. Menyimpan data ke dalam file CSV di lokasi yang telah ditentukan
    4. Mengirim lokasi penyimpanan data ke XCom untuk proses selanjutnya
    """
    
    # Membuat koneksi ke database 
    source_hook = PostgresHook(postgres_conn_id="postgres_airflow")  
    source_conn = source_hook.get_conn()  

    # Ekstraksi data tabel 'table_m3' 
    data_raw = pd.read_sql('SELECT * FROM table_m3', source_conn)

    # Menyimpan data mentah dalam format CSV
    path = '/opt/airflow/dags/_data_raw.csv' # sesuaikan path
    data_raw.to_csv(path, index=False)

    # Mengirim lokasi penyimpanan
    context['ti'].xcom_push(key='lokasi_data', value=path)



# Fungsi Transform
def transform(**context):
    """
    Fungsi transform digunakan untuk membersihkan, memvalidasi, dan mengubah data yang telah diekstrak 
    dari PostgreSQL agar lebih terstruktur dan konsisten. Proses ini memastikan bahwa data siap digunakan 
    untuk analisis lebih lanjut 
    
    Berikut merupakan tahapan yang dilakukan :
    1. Mengakses file CSV hasil ekstraksi dari PostgreSQL agar dapat diproses lebih lanjut.
    2. Menghapus kolom yang tidak diperlukan agar dataset lebih ringkas dan efisien.
    3. Missing value pada setiap kolom akan dihapus karena tidak berhubungan dengan kolom lain (MCAR) kemudian karena jumlahnya terbilang sedikit dari 
        total kesseluruhan data maka menghapus missing value tidak akan terlalu mempengaruhi hasil.
    4. Menghapus data yang terindikasi sebagai data  duplikat
    5. Mengubah kolom quantity dari numerik ke format integer agar lebih konsisten dalam analisis.
    6. Mengubah nama kolom menjadi huruf kecil dan mengganti spasi dengan garis bawah (_) agar lebih mudah digunakan.
    7. Menambahkan kolom indeks 'id' denganMembuat kolom 'id' sebagai nilai unik untuk setiap baris data.
    8. Menyimpan data hasil transformasi yang telah dibersihkan dalam file CSV untuk digunakan pada tahap berikutnya.   
    """
    
    # Mengambil path file hasil ekstraksi dari XCom
    ti = context['ti']
    data_path = ti.xcom_pull(task_ids='extract_data', key='lokasi_data') 
    
    # Membaca data ke dalam dataframe
    data_raw = pd.read_csv(data_path)

    # Menghapus kolom yang tidak diperlukan
    data_raw.drop(columns=[
        'Total Land Area (acres)', 'Storage Condition', 'Product ID', 'Production Date', 
        'Expiration Date', 'Minimum Stock Threshold (liters/kg)','Reorder Quantity (liters/kg)'
    ], inplace=True)
    
    
    # Menangani missing value
    cols = ['Number of Cows', 'Farm Size', 'Quantity (liters/kg)', 'Product Name', 'Brand', 'Sales Channel', 'Price per Unit', 'Total Value']
    data_raw.dropna(subset=cols, inplace=True)

    # Menghapus data yang duplikat
    data_raw.drop_duplicates(inplace=True)
    
    # Mengonversi tipe kolom numerik ke integer 
    int_columns = ['Number of Cows', 'Quantity (liters/kg)', 'Quantity Sold (liters/kg)', 'Quantity in Stock (liters/kg)']
    for col in int_columns:
        data_raw[col] = data_raw[col].astype(int, errors='ignore')
    
    # Mengubah nama kolom untuk lebih standar
    data_raw.rename(columns={
        'Quantity (liters/kg)': 'quantity',
        'Shelf Life (days)': 'shelf_life',
        'Quantity Sold (liters/kg)': 'quantity_sold',
        'Price per Unit (sold)': 'price_per_unit_sold',
        'Approx. Total Revenue(INR)': 'total_revenue',
        'Quantity in Stock (liters/kg)': 'quantity_in_stock',
    }, inplace=True)
    
    # Mengubah semua nama kolom menjadi huruf kecil dan mengganti spasi dengan underscore
    data_raw.columns = data_raw.columns.str.lower().str.replace(" ", "_")
    
    # Menambahkan kolom 'id' sebagai nilai uniq
    data_idx = len(data_raw)
    data_raw['id'] = range(1, data_idx + 1)

    # Menyimpan hasil transformasi ke dalam file CSV
    clean_path = '/opt/airflow/dags/data_clean.csv' # sesuaikan path
    data_raw.to_csv(clean_path, index=False)

    # Mengirim lokasi penyimpanan data yang sudah dibersihkan ke XCom
    context['ti'].xcom_push(key='lokasi_data_bersih', value=clean_path)

# Fungsi Load
def load(**context):
    """
    Fungsi ini digunakan untuk memuat data yang telah dibersihkan ke Elasticsearch.
    
    Berikut merupakan tahapan yang dilakukan    
    1. Mengambil lokasi file CSV hasil transformasi dari XCom untuk diproses lebih lanjut.
    2. Membuka koneksi ke Elasticsearch agar data dapat disimpan dan diindeks.
    3. Membaca file CSV yang telah dibersihkan dan mengonversinya menjadi DataFrame.
    4. Menentukan indeks di Elasticsearch sebagai tempat penyimpanan data.
    5. Mengonversi setiap baris data ke dalam format dictionary dan memuatnya ke dalam Elasticsearch menggunakan es.index().
    6. Menampilkan pesan konfirmasi setelah semua data berhasil dimuat.
    """

    # Mengambil lokasi file CSV hasil transformasi dari XCom
    ti = context['ti']
    data_path = ti.xcom_pull(task_ids='transform_data', key='lokasi_data_bersih')

    # Membuka koneksi ke Elasticsearch
    es = Elasticsearch(['http://elasticsearch:9200'])

    # Membaca file CSV hasil transformasi
    data_clean = pd.read_csv(data_path)

    # Menentukan nama indeks di Elasticsearch
    index_name = "goods_sales_clean"

    # Memasukkan setiap baris data ke dalam Elasticsearch
    for _, row in data_clean.iterrows():
        doc = row.to_dict()  
        es.index(index=index_name, body=doc)  

    # Konfirmasi bahwa data telah berhasil dimuat
    print(f"Data berhasil dimuat ke Elasticsearch dengan indeks '{index_name}'.")



# Membuat objek DAG
"""
DAG (Directed Acyclic Graph) dalam Apache Airflow adalah struktur yang digunakan untuk mengelola dan 
menjadwalkan alur kerja (workflow). DAG mengatur bagaimana setiap tugas (task) dieksekusi, menetapkan 
urutan pengerjaannya, serta menentukan ketergantungan antar tugas agar proses berjalan sesuai dengan yang diharapkan.

Urutan dags nya adalah
1. Extract - Mengambil data dari sumber
2. Transform - Membersihkan dan memproses data
3. Load - Memasukkan data ke  Elasticsearch
"""

with DAG(
    dag_id="pipeline_inka", 
    description="Pipeline untuk proses ETL M3",
    schedule_interval="10,20,30 9 * * 6",  # Menjalankan pada menit 10, 20, dan 30 setiap Sabtu pukul 09:00
    default_args=default_args,
    catchup=False
) as dag:

    # Task 1: Extract - Mengambil data dari sumber
    extract_data = PythonOperator(
        task_id = "extract_data",
        python_callable = extract, 
        provide_context = True
    )

    # Task 2: Transform - Membersihkan dan memproses data
    transform_data = PythonOperator(
        task_id = "transform_data",
        python_callable = transform,
        provide_context = True
    )

    # Task 3: Load - Memasukkan data ke  Elasticsearch
    load_data = PythonOperator(
        task_id = "load_data",
        python_callable = load,
        provide_context = True
    )

# Menentukan urutan eksekusi task
extract_data >> transform_data >> load_data