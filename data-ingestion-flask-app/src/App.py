from flask import Flask
from flask_google_cloud_logger import FlaskGoogleCloudLogger
import AlplProdDriver as driver
import AlplProdSapMaster as sapm
import AlplProdOrdersTrips as ordtrps
import AlplProdPortalMaster as portalm
import AlplProdJobCard  as jc
import AlplProdEmployee as em
import AlplProdVendor as vnd
import AlplDriverMining as dm
import AlplPortalMining as pm
import AlplTripMining as tm
import AlplDriverRmc as drmc
import AlplPortalRmc as prmc
import AlplTripRmc as trmc
import AlplOrdersCustomersRmc as ocrmc
from threading import Thread
import Util
from LoggingUtil import logger

app = Flask(__name__)

FlaskGoogleCloudLogger(app)



logger.info("Data Load Application")
logger.info("Database:"+Util.db+" Host:"+str(Util.eng_host))

@app.route('/')
def home():
    logger.info("Data load service is running")
    return 'Data load service is up and running!'

@app.route('/driver/<folder>/<file>',methods=['GET'])
def driver_data(folder,file):
    logger.info('Processing driver Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    driver_load = Thread(target = driver.main, args=(file_name,))
    driver_load.start()
    return('Driver Data processing Started')

@app.route('/portal/mining/<folder>/<file>',methods=['GET'])
def driver_data_mining(folder,file):
    logger.info('Processing Mining Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    if('mining' in file_name.lower() and 'driver' in file_name.lower()):
        driver_load = Thread(target = dm.main, args=(file_name,))
        driver_load.start()
    elif('mining' in file_name.lower() and ('trip' in file_name.lower() or 'dc_details' in file_name.lower())):
        trip_load = Thread(target = tm.main, args=(file_name,))
        trip_load.start()
    else:
        portal_load = Thread(target = pm.main, args=(file_name,))
        portal_load.start()
    return('Mining Data processing Started')

@app.route('/portal/rmc/<folder>/<file>',methods=['GET'])
def driver_data_rmc(folder,file):
    logger.info('Processing driver Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    if('rmc' in file_name.lower() and 'driver' in file_name.lower()):
        driver_load = Thread(target = drmc.main, args=(file_name,))
        driver_load.start()
    elif('rmc' in file_name.lower() and ('trip' in file_name.lower() or 'dc_details' in file_name.lower())):
        trip_load = Thread(target = trmc.main, args=(file_name,))
        trip_load.start()
    elif('rmc' in file_name.lower() and ('customer' in file_name.lower() or 'order' in file_name or 'calend' in file_name.lower())):
        trip_load = Thread(target = ocrmc.main, args=(file_name,))
        trip_load.start()
    else:
        portal_load = Thread(target = prmc.main, args=(file_name,))
        portal_load.start()
    return('Mining Data processing Started')

@app.route('/sap/<folder>/<file>',methods=['GET'])
def load_sap_master_data(folder,file):
    logger.info('Processing sap Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    if 'vendor' in file_name:
        sap_load = Thread(target=vnd.main, args=(file_name,))
        sap_load.start()
    else:
        sap_load = Thread(target=sapm.main, args=(file_name,))
        sap_load.start()
    return('Sap Data processing Started')

@app.route('/orders-trips/<folder>/<file>',methods=['GET'])
def load_orders_trips_data(folder,file):
    logger.info('Processing orders-trips Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    ord_load = Thread(target=ordtrps.main, args=(file_name,))
    ord_load.start()
    return('Orders-trips Data processing Started')

@app.route('/portal/<folder>/<file>',methods=['GET'])
def load_portal_data(folder,file):
    logger.info('Processing portal Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    portal_load = Thread(target=portalm.main, args=(file_name,))
    portal_load.start()
    return('Portal Data processing Started')

@app.route('/maintenance/<folder>/<file>',methods=['GET'])
def load_jobcard_data(folder,file):
    logger.info('Processing job card Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    portal_load = Thread(target=jc.main, args=(file_name,))
    portal_load.start()
    return('Job card Data processing Started')

@app.route('/employee/<folder>/<file>',methods=['GET'])
def load_employee_data(folder,file):
    logger.info('Processing Employee Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    portal_load = Thread(target=em.main, args=(file_name,))
    portal_load.start()
    return('Employee Data processing Started')

@app.route('/tyres-parts/<folder>/<file>',methods=['GET'])
def load_tyres_parts_data(folder,file):
    logger.info('Processing Tyres Parts Data')
    file_name=folder+'/'+file
    logger.info(file_name)
    portal_load = Thread(target=sapm.main, args=(file_name,))
    portal_load.start()
    return('Tyres Parts Data processing Started')

@app.route('/procedure-call/<name>',methods=['GET'])
def load_procedure_call(name):
    logger.info('Calling procedure')
    logger.info(name)
    proc = Thread(target=Util.call_procedure,args=(name,))
    proc.start()
    return('called the procedure:'+ name)

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8989)