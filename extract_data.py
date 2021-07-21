from pyhocon import ConfigFactory
from google.cloud import storage
import io
import psycopg2
import os


KEY_FILE = 'path_key'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_FILE
conf = ConfigFactory.parse_file('ingestion.conf').get_config('databases')
table_names = conf.get_list('table_names')
db = 'fakedata'
user = 'postgres'
password = 'postgres'
host = 'localhost'
source = 'postgres'
pw = 'postgres'
#Metadata
path = "raw/data/postgresql/"
zone = 'raw'
bucket_name = conf.get_string('bucketName')
query_list = conf.get_list('queries')
'''
'SELECT * FROM customers'
'SELECT * FROM products'
'SELECT * FROM transactions'
'SELECT * FROM stores'
'''
pathOldStorage = 'path_old_storage'
client = storage.Client()
bucket = client.get_bucket(bucket_name)
conn = psycopg2.connect(dbname=db, user=user, password=pw, host=host)
cur = conn.cursor()
table_num = len(table_names)
files = ['customers_debt.csv', 'crypto_manage.json']

def copyPostgres(bucket, select_query, filename, metadata):
    query = f"""COPY ({select_query}) TO STDIN \
            WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE)"""
    file = io.StringIO()
    cur.copy_expert(query, file)
    csvFile = file.getvalue()
    blob = bucket.blob(filename + '.csv')
    blob.upload_from_string(csvFile,'text/csv')
    metadata['path'] = blob.bucket.name + "/" + blob.name
    blob.metadata =  metadata
    blob.patch()

def data_ingestion():
    for num in range(table_num):
        count = num - 1
        select_query = query_list[count]
        filename = table_names[count]
        metadata = {'source': source, 'path': path, 'zone': zone}
        copyPostgres(bucket, select_query, path + filename, metadata)

def copy_files():
    path = "raw/data/old_storage/"
    sourceCopy = 'old_storage'

    for file in files:
        blob = bucket.blob(path + file)
        blob.upload_from_filename(pathOldStorage + file,'text/csv')
        pathCopy = blob.bucket.name + "/" + blob.name
        blob.metadata =  {'source': sourceCopy, 'path': pathCopy, 'zone': zone}
        blob.patch()

