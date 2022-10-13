from datetime import datetime
import Util
import MultiProc
from LoggingUtil import logger



currentTime = datetime.now();
currentTimestamp=str(currentTime)
currentTimestamp = "'"+currentTimestamp[:currentTimestamp.find('.')]+"'"
currentTimestamp='now()'


filename=''


def validate_string(val, val1):
    val=str(val)
    if val!=None and val.replace('.', '', 1).isdigit():
        if val != None and val != "":
            return val
        else:
            return 'NULL'

    if val!=None and ((val.find("'", 0, len(val)) >= 0)):
        val = val.replace('\'', '\\\'')
        return 'E' + "'" + val + "'"

    elif val1 == 'created_date_time' or val1 == 'stock_date_time':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y/%m/%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'

    elif val1 == 'effective_from' or val1 == 'effective_to':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'

    elif val1 == 'asset_reg_date' or val1 == 'asset_valid_till' or val1 == 'created_time' or val1 == 'asset_modf_date' or val1 == 'asset_dec_date' or val1 == 'expiry_date' or val1 == 'purchase_date' or val1 == 'decomiss_date' or val1 == 'fitment_date' or val1 == 'date' or val1 == 'grn_date' or val1 == 'indent_issued_date':

        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'

    elif val1 == 'tyre_purchase_date' or val1 == 'tyre_war_valid_date' or val1 == 'tyre_scrap_date' or val1 == 'rdate' or val1 == 'valid_to' or val1 == 'valid_from' or val1 == 'startDate' or val1 == 'start_date' or val1 == 'end_date':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'
    # elif val == 'NULL':
    #     return 'NULL'
    elif val != None and val != "" and val != 'NULL':
        return "'" + val + "'"

    else:
        return 'NULL'


def load_vendor_trans(json, source,con):
    logger.info("Data ingestion starting for alpl_vendor_transaction")
    # json_obj = getJsonFile(json,source)

    cursor = con.cursor()

    errors = 0
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'

    ctu = 0
    cti = 0
    p = 0
    q = None

    columMap = {
        'invoice_number': 'INVOICE_NO',
        'supplier_number': 'VENDOR',
        'invoice_date': 'INVOICE_DATE',
        'amount': 'AMOUNT',
        'balance':'BALANCE',
        'last_updated_date': ''
    }

    for i, item in enumerate(json):
        invno=item.get('INVOICE_NO',None)
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_vendor_transaction ('
            i = 0

            for table_column, json_column in columMap.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + validate_string(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)

                    if (table_column == 'created_by' or table_column == 'last_updated_by'):
                        value = 'SYSTEM'

                    if (table_column == 'last_updated_date'):
                        value = str(currentTimestamp)

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            # print(totalQuery)
            # print(dataset)
            cursor.execute(totalQuery)
        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_vendor_transaction', source, item, q, str(e),filename,invno)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()

    logger.info("Data ingestion completed. No of rows processed for alpl_vendor_transaction:"+str(p))
    logger.info("connection closed successfully")
    logger.info("inserted rows:"+str(p))
    logger.info("errors :"+str(errors))
    logger.info("updated rows:"+str(ctu))


def load_vendor_master(json, source,con):
    logger.info("Data ingestion starting for alpl_vendor_master")
    # json_obj = getJsonFile(json,source)

    cursor = con.cursor()

    cursor.execute('SELECT alpl_supplier_id from alpl_vendor_master')
    result = cursor.fetchall()
    supplierList = [int(i[0]) for i in result]
    # print(supplierList)
    errors = 0
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'

    ctu = 0
    cti = 0
    p = 0
    q = None
    valueID = '0'

    columMap = {
        'alpl_supplier_id': 'SUPPLIER_ID',
        'supplier_name': 'SUPPLIER_NAME',
        'start_date': 'START_DATE',
        'end_date': 'END_DATE',
        'group_name': 'GROUP',
        'created_by': '',
        'last_updated_by': '',
        # 'last_updated_date': '',
    }

    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_vendor_master ('
            updateQuery='Update alpl_vendor_master set '
            i = 0
            flag = 0
            supid=item.get('SUPPLIER_ID',None)
            for table_column, json_column in columMap.items():
                # print(str(int(validate_string(item.get(json_column, None), table_column))))
                if (flag == 1) or (table_column == 'alpl_supplier_id' and (
                        int(validate_string(item.get(json_column, None), table_column)) in supplierList)):

                    if flag == 0:
                        val = item.get(json_column, None)
                        # print(val)
                        if (type(val) in (int, float)):
                            val = str(val)
                        if (val == None):
                            val = ""
                        valueID = validate_string(val, table_column)
                        flag = 1
                    if i == 0:
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'created_by' or table_column == 'last_updated_by'):
                            value = 'SYSTEM'

                        # if (table_column == 'last_updated_date'):
                        #     value = currentTimestamp

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1
                else:
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'created_by' or table_column == 'last_updated_by'):
                            value = 'SYSTEM'

                        # if (table_column == 'last_updated_date'):
                        #     value = currentTimestamp

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            updateQuery = updateQuery + ' where alpl_vendor_master=' + valueID
            if (flag == 1):
                q = updateQuery
                # print(updateQuery)
                cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                # print(totalQuery)
                # print(dataset)
                cursor.execute(totalQuery)
                cti= cti + 1
        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_vendor_master', source, item, q, str(e),filename,supid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()

    logger.info("Data ingestion completed. No of rows processed for alpl_vendor_master:"+str(p))
    logger.info("connection closed successfully")
    logger.info("inserted rows:"+str(cti))
    logger.info("errors :"+str(errors))
    logger.info("updated rows:"+str(ctu))



def main(file_name):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # file = event
    # file = os.path.abspath('E:/json_files') + "/0714/model_test.json"
    try:
        # json_data = open(file).read()
        # json_obj = json.loads(json_data)
        global filename
        filename = file_name
        json_obj = Util.getJsonFile(Util.sap_master_bucket, file_name)
        logger.info("Processing file: " + file_name)
        # logger.info("Processing file: ")

        data = str(json_obj)
        data = data[2:8]
        sourcetype='PORTAL'
        if (data == 'Source'):
            dataSource = 'data'
            dest_bucket = Util.portal_processed_bucket
        elif (data == 'SOURCE'):
            dataSource = 'DATA'
            dest_bucket = Util.sap_processed_bucket
            sourcetype='SAP'
        jsonData = json_obj[dataSource]
        source = json_obj[data]
        if source == 'VENDOR_MASTER':
            func = load_vendor_master
        elif source == 'VENDOR_TRANSC':
            if len(jsonData)>1000:
                Util.truncate_table("alpl_vendor_transaction")
            func = load_vendor_trans
        MultiProc.runDataIngestion(jsonData, func, source)
        Util.move_blob(Util.sap_master_bucket, file_name, dest_bucket, file_name)
        #Util.call_error_data_url(filename, sourcetype)
    except Exception as ex:
        logger.info("Halting json data load processing due to following exception:" + str(ex))
        Util.move_blob(Util.sap_master_bucket, file_name, Util.error_bucket, file_name)
        Util.send_alert(file_name, str(ex))