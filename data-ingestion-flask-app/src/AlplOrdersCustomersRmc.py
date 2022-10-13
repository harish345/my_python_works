from datetime import datetime
import Util
import MultiProc
from LoggingUtil import logger
import json

org_id=3


def get_customer_id(cursor):
    cursor.execute("select alpl_customer_id, customer_id from alpl_rmc_customer_master")
    res_lt = cursor.fetchall()
    loct_map = dict(res_lt)
    return loct_map

def get_customer_id_from_customer_code(cursor):
    cursor.execute("select customer_code, customer_id from alpl_rmc_customer_master;")
    res_lt = cursor.fetchall()
    loct_map = dict(res_lt)
    return loct_map

def get_site_id_from_customer_site_code(cursor):
    cursor.execute("select site_code, site_id from alpl_rmc_customer_sites_master;")
    res_lt = cursor.fetchall()
    loct_map = dict(res_lt)
    return loct_map

def get_units(cursor):
    cursor.execute("select alpl_unit_code,unit_id from alpl_unit_master where org_id=3;")
    res_lt = cursor.fetchall()
    loct_map = dict(res_lt)
    return loct_map

def validate_string(val, val1):
    val=str(val)
    if val=='NULL' or val==None or val=="":
        return 'NULL'

    if val1 == 'trip_creation_time' or val1 == 'trip_close_time' or val1 == 'order_created_date' or val1 == 'order_created_date' or val1=='arrival_time' or val1=='departure_time' or val1=='created_date_time' or val1=='arrival_time' or val1=='departure_time' or val1=='created_date_time':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
            #return "'" + parse(val).strftime('%Y-%m-%d %H:%M:%S')+ "'"
        else:
            return 'NULL'
    elif val1 == 'from_date' or val1 == 'to_date' :
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
            #return "'" + parse(val).strftime('%Y-%m-%d %H:%M:%S')+ "'"
        else:
            return 'NULL'

    elif val != None and val != "":
        if "'" in val:
            return "'" + val.replace("'","''") + "'"
        return "'" + val + "'"
    else:
        return 'NULL'



def add_audit_columns(query, dataset, flag):

    current_timestamp = "now()"
    if flag:
        query += ", last_updated_time=" + current_timestamp + " , last_updated_by=" + "'SYSTEM' "
        return query + dataset

    else:
        query += ", created_time, created_by, last_updated_time, last_updated_by)"
        dataset += "," + current_timestamp + ",'SYSTEM', " + current_timestamp + ", 'SYSTEM')"
        return query + dataset


def load_rmc_customer_master(json_object,source,con):
    cursor = con.cursor()
    column_map = {
        "alpl_customer_id": "CustomerID",
        "customer_name": "Name",
        "customer_code": "CustomerCode",
        "customer_type": "CustomerType",
        "address_line_1": "AddressLine1",
        "address_line_2": "AddressLine2",
        "mobile": "RegMobileNo"
    }
    
    cursor.execute("select alpl_customer_id,customer_id from alpl_rmc_customer_master")
    result = cursor.fetchall()
    customers= dict(result)

    for item in json_object:
        try:
            i = 0
            insert_query = 'INSERT into alpl_rmc_customer_master  ('
            select_query = 'SELECT * FROM alpl_rmc_customer_master '
            update_query = 'UPDATE alpl_rmc_customer_master SET '
            where_clause = 'WHERE '
            dataset = 'VALUES ('
            flag = True

            for table_column, json_column in column_map.items():
                if i == 0:
                    insert_query += table_column
                    dataset += validate_string(item[json_column], table_column)
                    if table_column == 'alpl_customer_id':
                        where_clause += table_column + "=" + validate_string(item[json_column], table_column)
                    else:
                        update_query += table_column + "=" + validate_string(item[json_column], table_column)
                        flag = False
                    i = 1

                else:
                    insert_query += ',' + table_column
                    dataset += ',' + validate_string(item[json_column], table_column)
                    if flag:
                        update_query += table_column + "=" + validate_string(item[json_column], table_column) + " "
                        flag = False
                    else:
                        update_query += ', ' + table_column + "=" + validate_string(item[json_column],
                                                                                    table_column) + " "

            total_select_query = select_query + where_clause
            cursor.execute(total_select_query)
            result = cursor.fetchall()
            if result:
                total_update_query = add_audit_columns(update_query, where_clause, True)
                cursor.execute(total_update_query)
            else:
                total_query = add_audit_columns(insert_query, dataset, False)
                cursor.execute(total_query)
            con.commit()
        except Exception as ex:
            print(ex)


def load_rmc_customer_sites_master(json_object,source,con):
    cursor = con.cursor()
    column_map = {
        "customer_id": "CustomerID",
        "alpl_site_id": "SiteID",
        "site_name": "SiteName",
        "site_code": "SiteCode",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "address_line_1": "AddressLine1",
        "address_line_2": "AddressLine2",
    }

    alplid_to_id = get_customer_id(cursor)

    for item in json_object:
        try:
            i = 0
            insert_query = 'INSERT into alpl_rmc_customer_sites_master ('
            select_query = 'SELECT * FROM alpl_rmc_customer_sites_master '
            update_query = 'UPDATE alpl_rmc_customer_sites_master SET '
            where_clause = 'WHERE '
            dataset = 'VALUES ('
            flag = True

            for table_column, json_column in column_map.items():
                if i == 0:
                    insert_query += table_column
                    if table_column == 'customer_id':
                        dataset += validate_string(alplid_to_id[int(item[json_column])], table_column)
                    else:
                        dataset += validate_string(item[json_column], table_column)
                    if table_column == 'customer_id' or table_column == 'alpl_site_id':
                        if table_column == 'customer_id':
                            where_clause += table_column + "=" + validate_string(alplid_to_id[int(item[json_column])],
                                                                                 table_column)
                        else:
                            where_clause += table_column + "=" + validate_string(item[json_column], table_column)
                    else:
                        update_query += table_column + "=" + validate_string(item[json_column], table_column)
                        flag = False

                    i = 1

                else:
                    insert_query += ',' + table_column
                    if table_column == 'customer_id':
                        dataset += ',' + validate_string(alplid_to_id[int(item[json_column])], table_column)
                    else:
                        dataset += ',' + validate_string(item[json_column], table_column)
                    if table_column == 'customer_id' or table_column == 'alpl_site_id':
                        if table_column == 'customer_id':
                            where_clause += table_column + "=" + validate_string(alplid_to_id[int(item[json_column])],
                                                                                 table_column)
                        else:
                            where_clause += ' and ' + table_column + "=" + validate_string(item[json_column],
                                                                                           table_column)
                    else:
                        if flag:
                            update_query += table_column + "=" + validate_string(item[json_column], table_column) + " "
                            flag = False
                        else:
                            update_query += ', ' + table_column + "=" + validate_string(item[json_column],
                                                                                        table_column) + " "

            total_select_query = select_query + where_clause
            cursor.execute(total_select_query)
            result = cursor.fetchall()
            if result:
                total_update_query = add_audit_columns(update_query, where_clause, True)
                cursor.execute(total_update_query)
            else:
                total_query = add_audit_columns(insert_query, dataset, False)
                cursor.execute(total_query)
            con.commit()
        except Exception as error:
            con.rollback()
            print(error)
            
def load_rmc_calendar_blocking(json_object,source,con):
    cursor = con.cursor()
    
    if source=='RMC_CUSTOMER_ORDER':
        table = 'alpl_orders'
        column_map = {
        "inquiry_id": "InquiryId",
        "customer_id": "CustomerCode",
        "site_id": "CustomerSiteCode",
        "sales_order_id": "SalesOrderID",
        "order_id": "OrderID",
        "quotation_code": "QuotationCode",
        "product_code": "ProductCode",
        "grade": "Grade",
        "quantity": "OrderQty",
        "plant": "PlantCode",
        "employee_id":"SiteEnginner",
        'org_id':''
    }
    else:
        table='alpl_rmc_calendar_blocking'
        column_map = {
        "inquiry_id": "InquiryId",
        "customer_id": "CustomerCode",
        "site_id": "CustomerSiteCode",
        "sales_order_id": "SalesOrderID",
        "order_id": "OrderID",
        "quotation_code": "QuotationCode",
        "product_code": "ProductCode",
        "grade": "Grade",
        "order_quantity": "OrderQty",
        "plant_code": "PlantCode",
        "from_date": "FromDate",
        "to_date": "ToDate",
    }
    
    customer_id_dict = get_customer_id_from_customer_code(cursor)
    site_id_dict = get_site_id_from_customer_site_code(cursor)
    units = get_units(cursor)
    
    

    for item in json_object:
        insert_query = 'INSERT into '+table+ ' ('
        select_query = 'SELECT * FROM '+table+ '  '
        update_query = 'UPDATE '+table+ '  SET '
        where_clause = 'WHERE '
        dataset = 'VALUES ('
        i = 0
        flag = True
        where_clause_flag = True
        try:
            for table_column, json_column in column_map.items():
                if i == 0:
                    if table_column == 'customer_id':
                        insert_query += table_column
                        if item[json_column] in customer_id_dict.keys():
                            dataset += validate_string(customer_id_dict[item[json_column]], table_column)
                        else:
                            dataset += validate_string("", table_column)
                    elif table_column == 'site_id':
                        insert_query += table_column
                        if item[json_column] in site_id_dict.keys():
                            dataset += validate_string(site_id_dict[item[json_column]], table_column)
                        else:
                            dataset += validate_string("", table_column)
                    elif table_column == 'org_id':
                        insert_query += table_column
                        dataset += '3'
                    else:
                        insert_query += table_column
                        dataset += validate_string(item[json_column], table_column)

                    if table_column == 'order_id' or table_column == 'from_date' or table_column == 'to_date':
                        if where_clause_flag:
                            where_clause += table_column + "=" + validate_string(item[json_column], table_column)
                            where_clause_flag = False
                        else:
                            where_clause += ' and ' + table_column + "=" + validate_string(item[json_column],
                                                                                           table_column)
                    else:
                        if table_column == 'customer_id':
                            if item[json_column] in customer_id_dict.keys():
                                update_query += table_column + "=" + validate_string(customer_id_dict[item[json_column]], table_column)
                            else:
                                update_query += table_column + "=" + validate_string("", table_column)
                            flag = False
                        elif table_column == 'site_id':
                            if item[json_column] in site_id_dict.keys():
                                update_query += table_column + "=" + validate_string(site_id_dict[item[json_column]], table_column)
                            else:
                                update_query += table_column + "=" + validate_string("", table_column)
                            flag = False
                        elif table_column == 'org_id':
                            update_query += table_column
                            dataset += '3'
                            flag=False
                        else:
                            update_query += table_column + "=" + validate_string(item[json_column], table_column)
                            flag = False

                    i = 1

                else:
                    if table_column == 'customer_id':
                        insert_query += ',' + table_column
                        if item[json_column] in customer_id_dict.keys():
                            dataset += ',' + validate_string(customer_id_dict[item[json_column]], table_column)
                        else:
                            dataset += ',' + validate_string("", table_column)
                    elif table_column == 'site_id':
                        insert_query += ',' + table_column
                        if item[json_column] in site_id_dict.keys():
                            dataset += ',' + validate_string(site_id_dict[item[json_column]], table_column)
                        else:
                            dataset += ',' + validate_string("", table_column)
                    elif table_column == 'org_id':
                        insert_query += ',' + table_column
                        dataset += ',' +'3'
                    else:
                        insert_query += ',' + table_column
                        dataset += ',' + validate_string(item[json_column], table_column)
                    if table_column == 'order_id' or table_column == 'from_date' or table_column == 'to_date':
                        if where_clause_flag:
                            where_clause += table_column + "=" + validate_string(item[json_column], table_column)
                            where_clause_flag = False
                        else:
                            where_clause += ' and ' + table_column + "=" + validate_string(item[json_column],table_column)
                    else:
                        if flag:
                            if table_column == 'customer_id':
                                if item[json_column] in customer_id_dict.keys():
                                    update_query += table_column + "=" + validate_string(customer_id_dict[item[json_column]],table_column) + " "
                                else:
                                    update_query += table_column + "=" + validate_string("", table_column) + " "

                                flag = False
                            elif table_column == 'site_id':
                                if item[json_column] in site_id_dict.keys():
                                    update_query += table_column + "=" + validate_string(site_id_dict[item[json_column]],
                                                                                     table_column) + " "
                                else:
                                    update_query += table_column + "=" + validate_string("", table_column) + " "

                                flag = False
                            elif table_column == 'org_id':
                                update_query += table_column
                                dataset += '3'
                                flag=False
                            else:
                                update_query += table_column + "=" + validate_string(item[json_column], table_column) + " "
                                flag = False
                            
                        else:
                            if table_column == 'customer_id':
                                if item[json_column] in customer_id_dict.keys():
                                    update_query += ', ' + table_column + "=" + validate_string(customer_id_dict[item[json_column]],
                                                                                            table_column) + " "
                                else:
                                    update_query += ', ' + table_column + "=" +validate_string("", table_column) + " "
                            elif table_column == 'site_id':
                                if item[json_column] in site_id_dict.keys():
                                    update_query += ', ' + table_column + "=" + validate_string(site_id_dict[item[json_column]],
                                                                                            table_column) + " "
                                else:
                                    update_query += ', ' + table_column + "=" + validate_string("", table_column) + " "
                            elif table_column == 'org_id':
                                update_query += ', ' + table_column + "="+ '3'
                            else:
                                update_query += ', ' + table_column + "=" + validate_string(item[json_column],
                                                                                        table_column) + " "

            total_select_query = select_query + where_clause
            cursor.execute(total_select_query)
            result = cursor.fetchall()
            if result:
                total_update_query = add_audit_columns(update_query, where_clause, True)
                cursor.execute(total_update_query)
            else:
                total_query = add_audit_columns(insert_query, dataset, False)
                cursor.execute(total_query)
            con.commit()
        except Exception as error:
            con.rollback()
            print(error)

def load_rmc_order_target_trans(json_object,source,con):
    cursor = con.cursor()
    column_map = {
        "order_id": "OrderID",
        "total_time_in_minutes": "TotalTimeinMin",
        "total_time_per_trip_in_minutes": "TimePerTripinMin",
    }
    for item in json_object:
        try:
            i = 0
            insert_query = 'INSERT into alpl_rmc_order_target_trans   ('
            select_query = 'SELECT * FROM alpl_rmc_order_target_trans   '
            update_query = 'UPDATE alpl_rmc_order_target_trans   SET '
            where_clause = 'WHERE '
            dataset = 'VALUES ('
            flag = True
            where_clause_flag = True

            for table_column, json_column in column_map.items():
                if i == 0:
                    insert_query += table_column
                    dataset += validate_string(item[json_column], table_column)
                    if table_column == 'order_id':
                        if where_clause_flag:
                            where_clause += table_column + "=" + validate_string(item[json_column], table_column)
                            where_clause_flag = False
                        else:
                            where_clause += ' and ' + table_column + "=" + validate_string(item[json_column],
                                                                                           table_column)

                    else:
                        update_query += table_column + "=" + validate_string(item[json_column], table_column)
                        flag = False

                    i = 1

                else:
                    insert_query += ',' + table_column
                    dataset += ',' + validate_string(item[json_column], table_column)
                    if table_column == 'order_id':
                        if where_clause_flag:
                            where_clause += table_column + "=" + validate_string(item[json_column], table_column)
                            where_clause_flag = False
                        else:
                            where_clause += ' and ' + table_column + "=" + validate_string(item[json_column],
                                                                                           table_column)
                    else:
                        if flag:
                            update_query += table_column + "=" + validate_string(item[json_column], table_column) + " "
                            flag = False
                        else:
                            update_query += ', ' + table_column + "=" + validate_string(item[json_column],
                                                                                        table_column) + " "

            total_select_query = select_query + where_clause
            cursor.execute(total_select_query)
            result = cursor.fetchall()
            if result:
                total_update_query = add_audit_columns(update_query, where_clause, True)
                cursor.execute(total_update_query)
            else:
                total_query = add_audit_columns(insert_query, dataset, False)
                cursor.execute(total_query)
            con.commit()
        except Exception as error:
            con.rollback()
            print(error)

        except Exception as ex:
            print(ex)

def main(file_name):  # confirms that the code is under main function
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #file = event
    #file = os.path.abspath('E:/prod_dump') + "/0803/trips.json"
    try:
        #json_data = open('mtrl.json').read()
        #json_obj = json.loads(json_data)
        global filename
        filename = file_name
        json_obj = Util.getJsonFile(Util.rmc_portal_bucket, file_name)
        #logger.info(f"Processing file: {file['name']}.")
        logger.info("Processing file: "+ file_name)
        data = str(json_obj)
        dataKey=None
        if '[' in data:
            ch = data.find('[')
            dataKey = data[ch-7:ch-3]
        data = data[2:8]
        sourcetype='PORTAL'
        if (data == 'Source'):
            dataSource = 'data'
            dest_bucket = Util.rmc_processed_bucket
        elif (data == 'SOURCE'):
            dataSource = 'DATA'
            dest_bucket = Util.rmc_processed_bucket
            sourcetype='SAP'
        source = json_obj[data]
        func = None
        stored_proc = None
        if dataKey==None:
            jsn=[]
            jsdata = json_obj[dataSource]
            jsn.append(jsdata)
            jsonData=jsn
        else:
            jsonData = json_obj[dataKey]
        if source== 'RMC_CUSTOMER_MASTER':
            func = load_rmc_customer_master
        elif source == 'RMC_CALENDARBLOCKING'  or source=='RMC_CUSTOMER_ORDER':
            func = load_rmc_calendar_blocking
        elif source== 'RMC_CUSTOMERSITES_MASTER':
            func=load_rmc_customer_sites_master
        elif source == 'RMC_ORDERTARGET_TRANS':
            func=load_rmc_order_target_trans
        
        if func != None:
            MultiProc.runDataIngestion(jsonData, func,source)
        if stored_proc!=None:
            try:
                Util.call_procedure(stored_proc)
            except Exception as e:
                logger.error("Error calling stored proc:"+stored_proc+ str(e))
        Util.move_blob(Util.rmc_portal_bucket, file_name, dest_bucket, file_name)
        Util.call_error_data_url(filename, sourcetype)
    except Exception as ex:
        logger.error("Halting json data load processing due to following exception:" + str(ex))
        Util.move_blob(Util.rmc_portal_bucket, file_name, Util.error_bucket, file_name)
        Util.send_alert(file_name, str(ex))
