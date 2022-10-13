import os,json;
from google.cloud import storage
import sqlalchemy
from sqlalchemy import create_engine
import requests
from threading import Thread
from LoggingUtil import logger



#UAT
#driver_bucket='alpl_driver'
#sap_master_bucket = 'alpl-prod-sap-master'
#sap_processed_bucket = 'alpl-prod-sap-processed-files'
#trip_order_bucket='alpl_orders_trips_routes_assets'
#maintenance_bkt = 'alpl-prod-maintenance'
#employee_bkt='alpl-prod-employee'
#error_bucket='alpl-prod-errored-files'
#portal_bucket = 'alpl-prod-portal-master'
#portal_processed_bucket = 'alpl-prod-portal-processed-files'
#db='alpl_test' #UAT


driver_bucket = 'alpl-prod-driver'
sap_master_bucket = 'alpl-prod-sap-master'
sap_processed_bucket = 'alpl-prod-sap-processed-files'
trip_order_bucket = 'alpl-prod-orders-trips'
maintenance_bkt = 'alpl-prod-maintenance'
employee_bkt='alpl-prod-employee'
error_bucket='alpl-prod-errored-files'
portal_bucket = 'alpl-prod-portal-master'
tyres_parts_bucket='alpl-prod-tyres-parts'
portal_processed_bucket = 'alpl-prod-portal-processed-files'

#mining buckets
mining_portal_bucket='alpl-prod-portal-mining'
mining_processed_bucket='alpl-prod-mining-processed'
mining_sap_bucket='alpl-prod-sap-mining'

#rmc buckets
rmc_portal_bucket='alpl-prod-portal-rmc'
rmc_processed_bucket='alpl-prod-rmc-processed'
rmc_sap_bucket='alpl-prod-sap-mining'

template = 'dataIngestionError'
user='postgres'
password = 'admin'

#PROD
db='alpl_prod_db' #PROD
project='arunachala-hcc-030919'
region='asia-south1'
instance='alpl-dbinstance-prod-as1'
cloud_instance='arunachala-hcc-030919:asia-south1:alpl-dbinstance-prod-as1'
db_socket_dir='/cloudsql'


mailHost='http://api.arunachala.cc/alplapp'
mailUrl='/notification/alert'
errorDataUrl='/notification/alpl/post/errordata'

eng_url = sqlalchemy.engine.url.URL(
            drivername="postgres+pg8000",
            username=user,  # e.g. "my-database-user"
            password=password,  # e.g. "my-database-password"
            database=db,  # e.g. "my-database-name"
            query={
                "unix_sock": "{}/{}/.s.PGSQL.5432".format(
                    db_socket_dir,  # e.g. "/cloudsql"
                    cloud_instance)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
            }
        )

#UAT
eng_host=sqlalchemy.engine.url.URL(
            drivername="postgres+pg8000",
            username=user,  # e.g. "my-database-user"
            password=password,  # e.g. "my-database-password"
            #host='34.93.62.36', #UAT
            host='34.93.112.104',  # PROD
            port=5432,  # e.g. 5432
            database=db  # e.g. "my-database-name"
        )

engine = create_engine(eng_host,pool_size=25, max_overflow=75,pool_pre_ping=True,pool_recycle=7200)

engine_2 = create_engine(eng_host,pool_size=15, max_overflow=30,pool_pre_ping=True,pool_recycle=7200)


def getJsonFile(bucketname, filename_with_path):
    storage_client = storage.Client()
    bkt = storage_client.get_bucket(bucketname)
    destination_blob_pathname = filename_with_path
    blob = bkt.blob(destination_blob_pathname)
    return json.loads(blob.download_as_string())

def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):

    try:
        storage_client = storage.Client()
    
        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)
    
        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name)
        blob = source_bucket.blob(blob_name)
        blob.delete()
        logger.info("File moved to bucket :"+destination_bucket_name+". Name :"+destination_blob_name)
    except Exception as ex:
            logger.info("Failed to move file to bucket :"+destination_bucket_name+". Name :"+destination_blob_name)



class Singleton:
    def __init__(self, klass):
        self.klass = klass
        self.instance = None
    def __call__(self, *args, **kwds):
        if self.instance == None:
            self.instance = self.klass(*args, **kwds)
        return self.instance

class DBConnection(object):
    conn = None
    def get_connection(self):
        if self.conn is None:
            self.conn = engine.raw_connection()
        return self.conn
   
conn= DBConnection().get_connection()
def close_connection(con):
    if con!=None:
        con.close()
    
def truncate_table(table):
    connection = engine_2.connect()
    truncate_query = sqlalchemy.text("TRUNCATE TABLE "+table)
    connection.execution_options(autocommit=True).execute(truncate_query)
    connection.close()
    
def delete_rows(query):
    connection = engine_2.connect()
    delete_query = sqlalchemy.text(query)
    connection.execution_options(autocommit=True).execute(delete_query)
    connection.close()
    
def call_procedure(proc):
    connection = engine_2.connect()
    proc_query = sqlalchemy.text("CALL "+proc+"();")
    connection.execution_options(autocommit=True).execute(proc_query)
    connection.close()
    logger.info('Stored procedure executed :'+proc)
    
def insert_error_record(table_name,header_name,error_record,record,log,file,rkey=None):
    if log!=None and 'duplicate key' in log:
        pass
    else:
        con = engine_2.raw_connection()
        cursor = con.cursor()
        key='NULL'
        if error_record!=None:
            error_record = str(error_record).replace("'","''")
        if record!=None:
            record = record.replace("'","''")
        if log!=None:
            log = log.replace("'","''")
        if rkey!=None:
            key=rkey
        errorQuery = 'insert into alpl_data_ingestion_error_log(table_name,header_name,error_record,record,log,record_key,file_name) values('
        errorQuery = errorQuery+"'"+str(table_name)+"'"+","+"'"+str(header_name)+"'"+","+"'"+str(error_record)+"'"+","+"'"+str(record)+"'"+","+"'"+str(log)+"'"+","+"'"+str(key)+"'"+","+"'"+str(file)+"'"+')'
        cursor.execute(errorQuery)
        con.commit()
        con.close()
    
def send_alert(file,error):
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    values={'file':file,
            'error':error}
    request = {'to':'dba@arunachala.biz,',
               'cc':'suman.tammana@hitachivantara.com,harishkumar.kandakatla@hitachivantara.com,portal@arunachala.biz',
               'action':template,
               'channel':'DATA INGESTION CHANNEL',
               'language':'ENGLISH',
               'subject':'Error processing the file: '+file,
               'values':values}
    try:
        resp=requests.post(mailHost+mailUrl,data=json.dumps(request),headers=headers)
    except Exception as ex:
        logger.error("Error sending mail:" + str(ex))
        
def call_error_data_url(filename,source):
    t = Thread(target=send_error_data_alpl, args=(filename,source,))
    t.start()

def send_error_data_alpl(filename,source):
    try:
        resp=requests.get(mailHost+errorDataUrl+'/'+source+'/'+filename)
        #print(resp)
    except Exception as ex:
        logger.error("Error invoking alert data request:" + str(ex))
    