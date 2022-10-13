from datetime import datetime
import Util
import MultiProc
from LoggingUtil import logger
import json

#conn = Util.DBConnection().get_connection()

#con = Util.conn
#logger.info(con)
#cursor = con.cursor()

org_id=2

#logger.info(conn)

filename=''

def validate_string_vendor_location(val,val1,org,loc_map):
    val=str(val)
    if val1=='location_type_id' or val1=='from_location' or val1=='to_location':
        return loc_map.get(int(val),'NULL')
    elif val.replace('.', '', 1).isdigit():
        if val != None and val != "":
            return val
        else:
            return 'NULL'

    elif (val.find("'", 0, len(val)) >= 0):
        val = val.replace('\'', '\\\'')
        return 'E' + "'" + val + "'"

    elif val1=='arrival_time' or val1=='departure_time' or val1=='created_date_time':
        if val !=None and val!="":
            return "'"+datetime.strptime(val,'%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')+"'"
        else:
            return 'NULL'
    elif val1=='start_date' or val1=='end_date' and val=='0000-00-00':
        return 'NULL'
    elif val != None and val!="":
        return "'"+val+"'"

    else:
        return 'NULL'


def validate_string(val, val1):
    val=str(val)
    if val=='NULL' or val==None:
        return 'NULL'

    if val.replace('.', '', 1).isdigit() and val1 != 'int_indent_date' and val1!='dc_no':
        if val != None and val != "":
            return val
        else:
            return 'NULL'
    if val1 == 'trip_creation_time' or val1 == 'trip_close_time' or val1 == 'order_created_date' or val1 == 'order_created_date' or val1=='arrival_time' or val1=='departure_time' or val1=='created_date_time' or val1=='arrival_time' or val1=='departure_time' or val1=='created_date_time':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
            #return "'" + parse(val).strftime('%Y-%m-%d %H:%M:%S')+ "'"
        else:
            return 'NULL'
    elif val1 == 'bll_date' or val1 == 'dfd_created_date' or val1 == 'effective_from' or val1 == 'effective_to' or val1 == 'order_created_date' or val1 == 'dc_date' or val1 == 'order_completion_date':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
            #return "'" + parse(val).strftime('%Y-%m-%d %H:%M:%S')+ "'"
        else:
            return 'NULL'
    elif val1 == 'asset_reg_date' or val1 == 'asset_valid_till' or val1 == 'created_time' or val1 == 'asset_modf_date' or val1 == 'asset_dec_date' or val1 == 'pol_certificate_ldate' or val1 == 'pol_certificate_end_date' or val1 == 'nat_permit_end_date' or val1 == 'ins_expiry' or val1 == 'fit_certifcate_expiry_date' or val1 == 'ad_blue_fdate':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
            #return "'" + parse(val).strftime('%Y-%m-%d %H:%M:%S')+ "'"
        else:
            return 'NULL'

    elif val1 == 'ext_indent_date' or val1 == 'int_indent_date':
        if (val == '0000-00-00') or (val == 'None'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
            #return "'" + parse(val).strftime('%Y-%m-%d %H:%M:%S')+ "'"
        else:
            return 'NULL'

    elif val != None and val != "":
        if "'" in val:
            return "'" + val.replace("'","''") + "'"
        return "'" + val + "'"
    else:
        return 'NULL'

def get_emp_ids(cursor):
    cursor.execute('SELECT alpl_emp_id,emp_id FROM alpl_employee_master')
    result = cursor.fetchall()
    dictEmpID = dict(result)
    return dictEmpID

def get_route_ids(cursor):
    cursor.execute('SELECT route_id,CONCAT(from_location,to_location) FROM alpl_route_master where org_id=2')
    result = cursor.fetchall()
    dictRouteIdLoc = dict(result)
    return dictRouteIdLoc

def get_district(cursor):
    cursor.execute('SELECT upper(district),unit_district_id FROM alpl_unit_district_mapping')
    result = cursor.fetchall()
    dictDistrict=dict(result)
    return dictDistrict


def getDistrictId(district,dictDistrict):
    value = None
    if(district == None):
        value = ""
    else:
        district = district.upper()
        value = dictDistrict.get(district)

    if (value == None):
        value = ""
    elif (type(value) in (int, float)):
        value = str(value)
    return value

def getEmpId(alplEmpId,dictEmpID):
    if(alplEmpId == None):
        value = ""
    else:
        value = dictEmpID.get(int(alplEmpId))

    if(value == None):
        value = ""
    elif(type(value) in (int, float)):
        value = str(value)
    return value

def getRouteId(fromLoc,ToLoc,dictRouteIdLoc):
    val = fromLoc + ToLoc
    value = dictRouteIdLoc.get(val)
    if (value == None):
        value = ""
    elif (type(value) in (int, float)):
        value = str(value)
    return value

def load_trip_trans(json,source,con):
    logger.info("Data ingestion starting for alpl_trip_data")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    q = None
    ctu = 0
    cti = 0
    src = 'logistics'
    errors = 0
    if ("MINING" in source):
        src = 'mining'
    
    cursor.execute('select alpl_trip_id from alpl_trip_data where unit_id in(select unit_id from alpl_unit_master where org_id=2) and is_child_trip=False')
    result = cursor.fetchall()
    tripList = [i[0] for i in result]

    cursor.execute('SELECT alpl_driver_reference_id,alpl_driver_id from alpl_driver_list_master where org_id='+str(org_id))
    dr_result=cursor.fetchall()
    driverid_map = dict(dr_result)
    
    cursor.execute('select  unit_name,unit_id from alpl_unit_master where org_id='+str(org_id))
    result = cursor.fetchall()
    unit_map=dict(result)
    
    dictRouteIdLoc = get_route_ids(cursor)
   
    
    cursor.execute('select asset_reg_number,imei from alpl_asset_master')
    result = cursor.fetchall()
    asset_map=dict(result)

    columMap = {
                'alpl_trip_id': 'TripID',
                'is_completed': 'IsCompleted',
                'asset_reg_number': 'VehRegNo',
                 'driver_id': 'DriverID',
                 'route_from': 'RouteFrom',
                 'route_to': 'RouteTo',
                 'trip_creation_time': 'CreatedDateTime',
                 'trip_close_time': 'ClosedDateTime',
                 'advance_amount': 'AdvanceAmount',
                 'ad_blue': 'AdBlueQuantity',
                 'quantity': 'DieselQuantity',
                 'recovery': 'RecoveryAmount',
                 'exemption_amount': 'ExemptionAmount',
                 'budget': 'Budget',
                 'actual_expense': 'ActualExpenses',
                 'target_kmpl': 'TargetKmpl',
                 'actual_kmpl': 'ActualKmpl',
                 'actual_kms': 'ActualKms',
                 'target_liters': 'TargetDieselQuantity',
                 'internal_order_id':'InternalOrderId',
                 'master_route_id': '',
                 'unit_id':'LocationID',
                 'trip_order_type_id':'TripOrderTypeID',
                 'org_id':''
                 #'trip_id' : ''
                  }

    for i, item in enumerate(json):
        #logger.info('_________________________________________')
        dataset = 'values ('
        query = 'INSERT INTO alpl_trip_data ('
        updateQuery = 'UPDATE alpl_trip_data SET '
        i = 0
        flag = 0
        valueID = '0'
        fromLocation = None
        toLocation = None
        veh_reg = None
        trip_cr_date = None
        trip_ed_date = None
        internal_order_id = None
        IntOrderId = None
        IntIndentIssued=None
        IntIndentIssuedDate = None
        IntIndentPrice = None
        ExtOrderId = None
        ExtIndentIssued = None
        ExtIndentIssuedDate = None
        ExtIndentPrice = None
        rows_count = None
        updateFlag = 0
        serialTripId = None
        workFlowFlag = 0
        workFlowData = {}
        unt = str(item.get('TripID', None))
        asset = str(item.get('VehRegNo',None))
        completed=str(item.get('IsCompleted',None))
        try:
            for table_column, json_column in columMap.items():
                #logger.info('----------------------------------')
                if (flag==1) or (table_column == 'alpl_trip_id' and (
                        int(validate_string(item.get(json_column, None), table_column)) in tripList)):

                    if flag == 0:
                        value = str(item.get(json_column, None))
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None or value== 'None'):
                            value = ""
                        valueID = validate_string(value, table_column)
                        flag = 1
                    
                    if (i == 0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    else:
                        if (table_column != 'alpl_trip_id'):
                            updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
                        
                        if(table_column == 'driver_id'):
                            value = str(driverid_map.get(int(value),'NULL'))

                        if (table_column == 'is_completed'):
                            if (value.upper() == 'TRUE'):
                                value = 1
                                workFlowFlag = 1
                            elif (value.upper() == 'FALSE'):
                                value = 0
                        if (table_column == 'route_from'):
                            fromLocation = str(item.get(json_column, None))

                        if (table_column == 'route_to'):
                            toLocation = str(item.get(json_column, None))


                        if (table_column == 'master_route_id'):
                            value = getRouteId(fromLocation,toLocation,dictRouteIdLoc)
                            
                        if (table_column == 'unit_id'):
                            value = unit_map.get(item.get('RouteFrom'),'NULL')
                        
                        if (table_column == 'org_id'):
                            value = org_id

                        if (table_column == 'int_order_id'):
                            internalQuery = 'select order_id,indent_issued,indent_issued_date,indent_price'
                            internalQuery = internalQuery + " from alpl_diesel_indent_internal where vehicle_number = " + str(veh_reg)
                            internalQuery = internalQuery + " and indent_issued_date >= " + str(trip_cr_date) + " and indent_issued_date <= " + str(trip_ed_date)
                            internalQuery = internalQuery + " order by indent_issued_date desc limit 1"
                            #cursor.execute(internalQuery)
                            #records = cursor.fetchall()
                            #row = len(records)
                            row=None
                            records=None

                            if row == 1:
                                IntOrderId = records[0][0]
                                IntIndentIssued = records[0][1]
                                IntIndentIssuedDate = records[0][2]
                                IntIndentPrice = records[0][3]


                            value = IntOrderId

                        if (table_column == 'int_indent_issued'):
                            value = IntIndentIssued
                        if (table_column == 'int_indent_date'):
                            value = str(IntIndentIssuedDate)

                        if (table_column == 'int_price'):
                            value = IntIndentPrice

                        if (table_column == 'ext_order_id'):
                            externalQuery = 'select order_id,indent_issued,indent_issued_date,indent_price'
                            externalQuery = externalQuery + " from alpl_diesel_indent_external where vehicle_number = " + str(veh_reg)
                            externalQuery = externalQuery + " and indent_issued_date >= " + str(trip_cr_date) + " and indent_issued_date <= " + str(trip_ed_date)
                            externalQuery = externalQuery + " order by indent_issued_date desc limit 1"
                            #logger.info(externalQuery)
                            #cursor.execute(externalQuery)
                            #records = cursor.fetchall()
                            #row = len(records)
                            records=None
                            row=None

                            if row == 1:
                                ExtOrderId = records[0][0]
                                ExtIndentIssued = records[0][1]
                                ExtIndentIssuedDate = records[0][2]
                                ExtIndentPrice = records[0][3]


                            value = ExtOrderId

                        if (table_column == 'ext_indent_issued'):
                            value = ExtIndentIssued
                        if (table_column == 'ext_indent_date'):
                            value = str(ExtIndentIssuedDate)
                        if (table_column == 'ext_price'):
                            value = ExtIndentPrice
                        #if(table_column=='unit_id'):
                            #value=unit_map.get(item.get('RouteFrom').split(',')[0])
                        if (type(value) in (int, float)):
                            value = str(value)

                        if (value == None):
                            value = ""
                        if (table_column == 'asset_reg_number'):
                            veh_reg = validate_string(value, table_column)

                        if (table_column == 'trip_creation_time'):
                            trip_cr_date = validate_string(value, table_column)

                        if (table_column == 'trip_close_time'):
                            trip_ed_date = validate_string(value, table_column)

                        if(table_column != 'trip_id'):
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

                        if(table_column == 'driver_id'):
                            value = str(driverid_map.get(int(value),'NULL'))
                            #if value ==None:
                            #   raise Exception("Driver id"+item.get(json_column, None)+" doesnot exist in drivers data. Trip:" +str(unt))
                        if (table_column == 'is_completed'):
                            if (value.upper() == 'TRUE'):
                                value = 1
                                workFlowFlag = 1
                            elif (value.upper() == 'FALSE'):
                                value = 0

                        if (table_column == 'route_from'):
                            fromLocation = str(item.get(json_column, None))

                        if (table_column == 'route_to'):
                            toLocation = str(item.get(json_column, None))

                        if (table_column == 'master_route_id'):
                            value = getRouteId(fromLocation, toLocation,dictRouteIdLoc)
                        
                        if (table_column == 'unit_id'):
                            value = unit_map.get(item.get('RouteFrom'),'NULL')
                            
                        if (table_column == 'org_id'):
                            value = org_id

                        if(table_column == 'int_order_id'):
                            internalQuery = 'select order_id,indent_issued,indent_issued_date,indent_price'
                            internalQuery = internalQuery + " from alpl_diesel_indent_internal where vehicle_number = "+str(veh_reg)
                            internalQuery = internalQuery + " and indent_issued_date >= " + str(trip_cr_date)+" and indent_issued_date <= "+ str(trip_ed_date)
                            internalQuery = internalQuery + " order by indent_issued_date desc limit 1"
                            #logger.info(internalQuery)
                            #cursor.execute(internalQuery)
                            #records = cursor.fetchall()
                            #row = len(records)
                            row=None

                            if row == 1:
                                IntOrderId = records[0][0]
                                IntIndentIssued = records[0][1]
                                IntIndentIssuedDate = records[0][2]
                                IntIndentPrice = records[0][3]

                            value = IntOrderId


                        if (table_column == 'int_indent_issued'):
                            value = IntIndentIssued
                        if (table_column == 'int_indent_date'):
                            value = str(IntIndentIssuedDate)
                        if (table_column == 'int_price'):
                            value = IntIndentPrice

                        if (table_column == 'ext_order_id'):
                            externalQuery = 'select order_id,indent_issued,indent_issued_date,indent_price'
                            externalQuery = externalQuery + " from alpl_diesel_indent_external where vehicle_number = " + str(veh_reg)
                            externalQuery = externalQuery + " and indent_issued_date >= " + str(trip_cr_date) + " and indent_issued_date <= " + str(trip_ed_date)
                            externalQuery = externalQuery + " order by indent_issued_date desc limit 1"
                            #logger.info(externalQuery)
                            #cursor.execute(externalQuery)
                            #records = cursor.fetchall()
                            #row = len(records)
                            row=None

                            if row == 1:
                                ExtOrderId = records[0][0]
                                ExtIndentIssued = records[0][1]
                                ExtIndentIssuedDate = records[0][2]
                                ExtIndentPrice = records[0][3]

                            value = ExtOrderId


                        if (table_column == 'ext_indent_issued'):
                            value = ExtIndentIssued
                        if (table_column == 'ext_indent_date'):
                            value = str(ExtIndentIssuedDate)
                        if (table_column == 'ext_price'):
                            value = ExtIndentPrice
                        #if(table_column=='unit_id'):
                            #value=unit_map.get(item.get('RouteFrom').split(',')[0])

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""

                        if (table_column == 'asset_reg_number'):
                            veh_reg = validate_string(value, table_column)

                        if (table_column == 'trip_creation_time'):
                            trip_cr_date = validate_string(value, table_column)

                        if (table_column == 'trip_close_time'):
                            trip_ed_date = validate_string(value, table_column)

                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' ,last_updated_time=now() where alpl_trip_id=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                #logger.info(q)
                cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                #logger.info(q)
                cursor.execute(totalQuery)
                cti = cti + 1
        except Exception as e:
            logger.info(str(e)+str(i))
            logger.info(q)
            con.rollback()

            try:
                pass
                Util.insert_error_record('alpl_trip_data', source, item, q, str(e),filename,unt)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1

        else:
            con.commit()
            if completed=='False' and unt!=None and asset!=None and asset_map!=None:
                pass
                #insert_asset_telemetry(unt, asset_map, asset)
            # if (workFlowFlag == 1):
            #     workFlowData['assetRegNumber'] = str(veh_reg)
            #     unt = int(unt[0:4])
            #     workFlowData['unit'] = unt
            #     auth_token = 'd087db24-d28f-4998-99e9-63f9e5ff4ac2'
            #     hed = {'Authorization': 'Bearer ' + auth_token}
            #     requests.post("http://localhost:3000/lifecycle/start", params=workFlowData, headers=hed)
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_trip_data:"+str(p))
    logger.info("number of updated rows :"+str(ctu))
    logger.info("number of inserted rows :"+str(cti))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")



def load_trip_dc(json,source,con):
    logger.info("Data ingestion starting for alpl_trip_dc")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    ctu = 0
    cti =0
    wst = 0
    #dc_id = 0
    errors = 0
    src = 'mining'
    cursor.execute('SELECT dc_no FROM alpl_dc_details where unit_id in (select unit_id from alpl_unit_master where org_id=2)')
    result = cursor.fetchall()
    dc_list = [i[0] for i in result]
    
    cursor.execute('select alpl_trip_id,trip_id from alpl_trip_data where unit_id in(select unit_id from alpl_unit_master where org_id=2) and is_child_trip=False')
    result = cursor.fetchall()
    trip_map = dict(result)
    
    cursor.execute('SELECT alpl_driver_reference_id,alpl_driver_id from alpl_driver_list_master where org_id='+str(org_id))
    dr_result=cursor.fetchall()
    driverid_map = dict(dr_result)
    
    src = 'mining'
    columMap = {'dc_no': 'DCNo',
                'trip_id': 'TripID',
                'vehicle_reg_no': 'VehRegNo',
                #'alpl_driver_id': 'DriverID',
                'order_id': 'OrderID',
                'quantity': 'Quantity',
                'dc_date': 'dcdate',
                'incentive_amount': 'IncentiveAmount',
                'dc_type_id':'DcTypeID',
                'unit_id':'LocationID'}

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dc_details ('
        updateQuery = 'UPDATE alpl_dc_details SET '
        i = 0
        #flag = 0
        valueID = '0'
        q = None
        #dc_id = dc_id + 1
        dcno = item.get('DCNo',None)
        try:
            if(item.get('DCNo') in dc_list):
                #logger.info('DC exists')
                temp = item.get('TripID', None)
                if (temp == None):
                    value = ""
                else:
                    tripId = trip_map.get(int(temp))
                    if (tripId == None):
                        value = 'NULL'
                    else:
                        value = tripId
                #driver = driverid_map.get(int(item.get('DriverID')),'NULL')
                qty=item.get('Quantity',None)
                if qty=='' or qty==None:
                    qty='null'
                updateQuery = updateQuery +'trip_id = '+str(value)+' ,dc_date='+validate_string(item.get('dcdate',None), 'dc_date')+ ', quantity = '+ str(qty)+' ,dc_type_id='+str(item.get('DcTypeID','NULL'))+' ,last_updated_time=now() where dc_no = '+ "'"+str(item.get('DCNo'))+"'"
                #logger.info(updateQuery)
                cursor.execute(updateQuery)
                con.commit()
                ctu = ctu+1
                continue
            else:
                for table_column, json_column in columMap.items():
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)
                        
                        if (type(value) in (int, float)):
                            value = str(value)
                        elif (value == None):
                            value = ""
    
                        dataset = dataset + validate_string(value, table_column)
    
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
    
                        if (table_column == 'unit_id' and src == 'mining'):
                            if(value == '1'):
                                value = 8
                            elif(value == '2'):
                                value = 7
    
                        if (table_column == 'trip_id'):
                            tripId = trip_map.get(int(value))
                            if (tripId == None):
                                value = 'NULL'
                            else:
                                value = tripId
    
                        if (table_column == 'alpl_driver_id'):
                            value = driverid_map.get(int(value))
    
                        if (table_column == 'dc_type_id'):
                            if (value==0):
                                wst = wst + 1
    
                        if (type(value) in (int, float)):
                            value = str(value)
                        elif (value == None):
                            value = ""
    
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1
                dataset = dataset + ')'
                query = query + ')'
                #updateQuery = updateQuery + ' where dc_no=' + valueID
                totalQuery = query + dataset
    
                #logger.info(totalQuery)
                cursor.execute(totalQuery)
                cti = cti+1
        except Exception as e:
            logger.info(str(e)+str(i))
            logger.info(q)
            con.rollback()

            try:
                Util.insert_error_record('alpl_dc_details', source, item, q, str(e),filename,dcno)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_trip_dc:"+str(p))
    logger.info("number of updated rows :"+str(ctu))
    logger.info("number of inserted rows :"+str(cti))
    logger.info("count of dc_type_id with 0 : "+str(wst))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")

def route_kmpl(json,source,con):
    logger.info("Data ingestion starting for alpl_route_kmpl")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()

    cursor.execute('SELECT route_path_id,route_id FROM alpl_route_master')
    result = cursor.fetchall()
    routePathId = [i[0] for i in result]
    routeId = [i[1] for i in result]

    dictRoute = {}
    length = len(routePathId)
    for x in range(length):
        dictRoute[routePathId[x]] = routeId[x]

    cursor.execute('SELECT alpl_asset_model_id,asset_model_id FROM alpl_asset_model')
    result = cursor.fetchall()
    assetmodelId = [i[0] for i in result]
    SerialAssetId = [i[1] for i in result]

    dictAssetmodel = {}
    length = len(assetmodelId)
    for x in range(length):
        dictAssetmodel[assetmodelId[x]] = SerialAssetId[x]
    p = 0
    q = None
    src = 'logistics'
    errors = 0
    columMap = {'km_from': 'KMFrom',
                'route_path_id': 'RoutePathID',
                'asset_model_id': 'VehicleModelID',
                'km_to': 'KMTo',
                'tonnage_from': 'TonnageFrom',
                'tonnage_to': 'TonnageTo',
                'kmpl': 'kmpl',
                'effective_from': 'EffectiveFrom',
                'effective_to': 'EffectiveTo',
                'master_route_id':''}

    p = 0
    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_route_kmpl_master ('
        i = 0
        flag = 0
        route_ID = None
        try:
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

                    if (table_column == 'route_path_id'):
                        route_ID = dictRoute.get(int(value))
                    if(table_column == 'master_route_id'):
                        value = route_ID

                    if (table_column == 'asset_model_id'):
                        value = dictAssetmodel.get(int(value))

                    if (value == None):
                        value = ""
                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            #logger.info(totalQuery)
            cursor.execute(totalQuery)
        except Exception as e:
            logger.info(e)
            con.rollback()
            try:
                if (type(q) in (int, float)):
                    q = str(q)
                elif (q == None):
                    q = ""
                if (type(e) in (int, float)):
                    e = str(e)
                elif (e == None):
                    e = ""
                if (type(src) in (int, float)):
                    src = str(src)
                elif (src == None):
                    src = ""
                errorQuery = 'insert into alpl_data_ingestion_error_log(table_name,header_name,record,log) values('
                errorQuery = errorQuery + validate_string('alpl_route_kmpl_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + str(validate_string(e, 'tab')) + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                logger.info(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            #logger.info("data inserted successfully")
            p = p + 1
            con.commit()
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_route_kmpl_master:", p)
    logger.info("errors :",errors)
    logger.info("connection closed successfully")

def trip_diesel(json,source,con):
    logger.info("Data ingestion starting for alpl_trip_diesel")
    #json_obj = getJsonFile(json,source)
    p = 0
    errors = 0
    q = None
    src = None
    cursor = con.cursor()
    cursor.execute('SELECT alpl_trip_id FROM alpl_trip_data')
    result = cursor.fetchall()
    tripList = [i[0] for i in result]
    columMap = {
        'trip_id': 'TripID',
        'vehicle_reg_number': 'VehRegNo',
        'driver_id': 'DriverID',
        'bll_date': 'billdate',
        'ind_qty': 'IndQty',
        'add_blue_ind_qty': 'AddBlueIndQty',
        'quantity': 'Quantity',
        'add_blue_qty': 'AddBlueQuantity',
        'petrol_bunk_id': 'PetrolBunkID',
        'dfd_created_date': 'DFDCreateddate'}

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_diesel_details ('
        i = 0
        IdQuery = "select max(diesel_detail_id) from alpl_diesel_details"
        cursor.execute(IdQuery)
        maxId = cursor.fetchone()
        if (maxId[0] == None):
            maxId = 0
        else:
            maxId = maxId[0]+1
        try:
            for table_column,json_column in columMap.items():
                if i==0:
                    query=query+table_column
                    value = item.get(json_column, None)
                    if (value == None):
                        value = ""

                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset=dataset+validate_string(value,table_column)
                else:
                    query=query+','+table_column
                    value = item.get(json_column, None)

                    if (value == None):
                        value = ""
                    if (type(value) in (int, float)):
                        value = str(value)

                    value = validate_string(value, table_column)
                    dataset=dataset+','+value

                i=1
            dataset=dataset+ ',' +str(maxId)+')'
            query=query+',diesel_detail_id'+')'
            totalQuery=query+dataset
            cursor.execute(totalQuery)
        except Exception as e:
            logger.info(e)
            con.rollback()
            try:
                if (type(q) in (int, float)):
                    q = str(q)
                elif (q == None):
                    q = ""
                if (type(e) in (int, float)):
                    e = str(e)
                elif (e == None):
                    e = ""
                if (type(src) in (int, float)):
                    src = str(src)
                elif (src == None):
                    src = ""
                errorQuery = 'insert into alpl_data_ingestion_error_log(table_name,header_name,record,log) values('
                errorQuery = errorQuery + validate_string('alpl_activity_master', 'tab') + ',' + str(
                    validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + str(validate_string(e, 'tab')) + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                logger.info(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p=p+1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_trip_diesel:", p)
    logger.info("errors :", errors)
    logger.info("connection closed successfully")


def load_trip_route_txn(json,source,con):
    logger.info("Data ingestion starting for alpl_trip_route_trans")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    errors = 0
    q=''
    cursor.execute('select alpl_trip_id from alpl_trip_data where unit_id in(select unit_id from alpl_unit_master where org_id=2) and is_child_trip=False')
    result = cursor.fetchall()
    tripList = [i[0] for i in result]
    
    cursor.execute('''SELECT distinct alpl_trip_id || '-' || route_path_id FROM alpl_trip_route_trans where extract(year from created_time)=extract(year from current_date)''')
    result = cursor.fetchall()
    trlist = [i[0] for i in result]
    
    cursor.execute('SELECT route_path_id,route_id FROM alpl_route_master where org_id=2')
    result = cursor.fetchall()
    route_path_map=dict(result)
    for i, item in enumerate(json):
        trip_id = item.get("TripID", None)
        route_path_id =item.get("RoutePathID", None)
        updateQuery = 'insert into alpl_trip_route_trans(alpl_trip_id,route_path_id,created_time,last_updated_time) values('
        try:
            if(str(trip_id)+'-'+str(route_path_id) in trlist):
                pass
            elif(int(trip_id) in tripList and int(route_path_id) in route_path_map.keys() and trip_id!=None and route_path_id!=None):
                updateQuery = updateQuery + str(trip_id)+','+str(route_path_id)+',now(),now() )'
                q=updateQuery
                cursor.execute(updateQuery)
            else:
                raise Exception('trip or route path doesnot exist, Trip:'+str(trip_id)+' route:'+str(route_path_id))
        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                pass
                Util.insert_error_record('alpl_trip_route_trans', source, item, q, str(e),filename,trip_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p+1
    
    try:
        tripUpdate="update alpl_trip_route_trans tr set trip_id=t.trip_id from alpl_trip_data t where t.alpl_trip_id=tr.alpl_trip_id and t.is_child_trip=False and tr.trip_id is null"
        cursor.execute(tripUpdate)
    except Exception as exp:
        logger.error(str(exp))
    else:
        con.commit()
        
    try:    
        tripRouteUpdate="""update alpl_trip_data td
        set master_route_id = troute.route_id
        from (select tr.alpl_trip_id,r.route_id from alpl_trip_route_trans tr,alpl_route_master r
        where r.route_path_id = tr.route_path_id
        and alpl_trip_id is not null
        and tr.created_time=(select max(created_time) from alpl_trip_route_trans
                         where route_path_id=tr.route_path_id and alpl_trip_id=tr.alpl_trip_id)) troute
        where troute.alpl_trip_id = td.alpl_trip_id"""
        
        #cursor.execute(tripRouteUpdate)
    except Exception as exp:
        logger.error(str(exp))
    else:
        con.commit()
    
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_trip_route:"+str(p))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")
    

def load_trip_route_leg_trans(json,source,con):
    logger.info("Data ingestion starting for alpl_trip_route_legs_transaction")
    #json_obj = getJsonFile(json,source)
    tripRouteLegLists = []
    trips=[]
    tripset=set()
    cursor = con.cursor()
    src = 'mining'
    orgId = 2
    
    delete_trips=[]
    
    for item in json:
        if item.get("TripID",None)!= None:
            tripset.add(int(item.get("TripID")))
    for t in tripset:
        #if t in trips:
        delete_trips.append(t)
    
    if len(delete_trips)>0:
        dql_q = "delete from alpl_trip_route_legs_transaction where alpl_trip_id in("
        drvs=','.join(str(v) for v in delete_trips)
        dql_q = dql_q+drvs+")"
        Util.delete_rows(dql_q)

    ctu = 0
    cti = 0
    p = 0
    sts = 0
    legQ=""
    mining=False
    errors = 0
    #cursor.execute('SELECT alpl_trip_id FROM alpl_trip_data where unit_id in (select unit_id from alpl_unit_master where org_id=2) and is_child_trip=False and is_completed=1')
    #result = cursor.fetchall()
    #trips = [i[0] for i in result]
    columMap={'alpl_trip_id':'TripID',
              'route_path_id':'RoutePathID',
              'sequence_num':'SeqNo',
              'leg_id':'LegID',
              'distance':'Distance',
              'target_duration_min':'TargetDurationinMin',
              'arrival_time':'ArrivalDateTime',
              'departure_time':'DepartureDateTime'}

    statusDict={'UNDEPT':'AT UNIT','DEPTDEPT':'AT UNIT','DEPTFR':'AT FACTORY','FRACT':'ACTIVITY','ACTFR':'FACTORY OUT','FRCO':'TOWARDS CONTROL POINT','CODEPT':'TOWARDS UNIT'}
    towardsDict={'FR':'TOWARDS FACTORY','DEPT':'TOWARDS UNIT','GDN':'TOWARDS GODOWN','CO':'TOWARDS CONTROL POINT','DLR':'TOWARDS DEALER','CONDES':'TOWARDS CONSIGNEE'}
    updatedStatusDict={'UNDEPT':'At Unit','DEPTDEPT':'At Unit','DEPTFR':'At Factory'}
    legQ='select alpl_leg_id,leg_master_id from alpl_leg_master where org_id='+str(orgId)
    mining=True
    
    cursor.execute(legQ)
    rls = cursor.fetchall()
    legs = dict(rls)

    for i, item in enumerate(json):

        dataset='values ('
        query='INSERT INTO alpl_trip_route_legs_transaction ('
        updateQuery = 'UPDATE alpl_trip_route_legs_transaction SET '
        i = 0
        flag = 0
        valueID = '0'
        tripID = 0
        routhPathId = 0
        legId = 0
        seqNo=0
        q=None
        leg=0
        checkValue = None
        trip=item.get('TripID',None)
        try:
            for table_column,json_column in columMap.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if(table_column == 'leg_id' and value is not None):
                        value = legs.get(int(value),None)
                        if value==None:
                            raise Exception("Leg Id not present in db "+ str(legId))
                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset = dataset + validate_string(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)
                    if(table_column == 'leg_id' and value is not None):
                        value = legs.get(int(value),None)
                        if value==None:
                            raise Exception("Leg Id not present in db "+ str(legId))
                    if (value == None):
                            value = ""
                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset = dataset + ',' + validate_string(value, table_column)
                i = 1

            dataset=dataset+')'
            query=query+') '
            totalQuery=query+dataset
            updateQuery = updateQuery + ',last_updated_time=now() where alpl_trip_id=' + str(tripID) + ' and route_path_id=' + str(routhPathId) + ' and leg_id=' + str(leg) + ' and sequence_num='+ str(seqNo)
            if (flag == 1):
                q = updateQuery
                cursor.execute(updateQuery)
                ctu = ctu + 1
                #logger.info(q)
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                cti = cti + 1
        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_trip_route_legs_transaction', source, item, q, str(e),filename,trip)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p=p+1
    updateTrip = 'update alpl_trip_route_legs_transaction l set trip_id = t.trip_id  from alpl_trip_data t  where t.alpl_trip_id=l.alpl_trip_id  and l.trip_id is null'
    cursor.execute(updateTrip)
    con.commit()
    con.close()
    #Util.call_procedure('update_leg_status')
    logger.info("Data ingestion completed. No of rows processed for alpl_trip_route_legs_transaction:"+str(p))
    logger.info("connection closed successfully")
    logger.info("inserted rows:"+str(cti))
    logger.info("updated rows:"+str(ctu))
    logger.info("status:"+str(sts))
    logger.info("error:"+str(errors))

def trip_expense_trans(json, source,con):
    logger.info("Data ingestion starting for alpl_trip_expense")
    # json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'
    p = 0
    errors = 0
    column_map = {
        'trip_id': 'TripID',
        'route_path_id': 'RoutePathID',
        'gl_expense_id':'Expense_GLID',
        'expense_desc':'ExpenseDesc',
        'budget': 'Budget',
        'actual': 'Actual'

    }
    uct = 0
    ict = 0
    tripExpQuery = 'select concat(trip_id,route_path_id,gl_expense_id) from alpl_prod.alpl_trip_expense'
    cursor.execute(tripExpQuery)
    result = cursor.fetchall()
    tripExpList = [i[0] for i in result]

    cursor.execute('SELECT distinct alpl_trip_id,trip_id FROM alpl_trip_data')
    result = cursor.fetchall()
    trip_map = dict(result)

    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_prod.alpl_trip_expense ('
        updateQuery = 'UPDATE alpl_prod.alpl_trip_expense SET '
        i = 0
        flag = 0
        valueID = None
        custId = None
        custIdCheck = None
        locId = None
        grade = None
        gradeCheck = None
        checkValue = None
        q = None


        trip_id = item.get('TripID', None)
        if (trip_id == ''):
            trip_id = ''
        else:
            trip_map.get(int(trip_id))
            trip_id = str(trip_map.get(int(trip_id)))

        rid = item.get('RoutePathID', None)
        if (rid == ''):
            rid = ''
        else:
            rid = str(rid)

        glid = item.get('Expense_GLID', None)
        if (glid == ''):
            glid = ''
        else:
            glid = str(glid)

        checkValue = trip_id+rid+glid

        try:
            for table_column, json_column in column_map.items():

                if (flag == 1) or ( table_column=='trip_id' and ((checkValue) in tripExpList)):
                    if flag == 0:
                        value = item.get(json_column, None)
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        valueID = validate_string(value, table_column)
                        flag = 1
                    if (i == 0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'trip_id'):
                            tripId = trip_map.get(int(value))
                            if (tripId == None):
                                value = 'NULL'
                            else:
                                value = tripId

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1
                else:
                    if (i == 0):
                        query = query + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'trip_id'):
                            tripId = trip_map.get(int(value))
                            if (tripId == None):
                                value = 'NULL'
                            else:
                                value = tripId

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + validate_string(value, table_column)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1




            updateQuery = updateQuery + ' where trip_id = ' + str(trip_id) + ' and route_path_id= '+ str(rid)+' and gl_expense_id='+str(glid)

            if (flag == 1):
                q = updateQuery
                cursor.execute(updateQuery)
                uct = uct + 1

            else:
                dataset = dataset+ ')'
                query= query+')'
                query = query+dataset
                q = query
                cursor.execute(query)
                ict = ict+1

        except Exception as e:
            logger.info(str(e) + str(i))
            logger.info(q)
            con.rollback()

            try:
                Util.insert_error_record('alpl_trip_expense', source, item, q, str(e), filename)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_trip_expense:"+ str(p))
    logger.info("inserted :"+str(ict))
    logger.info("updated :"+ str(uct))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")


def load_trip_driver_trans(json,source,con):
    logger.info("Data ingestion starting for alpl_mining_trip_driver_trans")
    #json_obj = getJsonFile(json,source)
    p = 0
    errors = 0
    q = None
    src = None
    cursor = con.cursor()
    cursor.execute('SELECT alpl_trip_id,trip_id FROM alpl_trip_data where org_id=2 and is_child_trip=False')
    result = cursor.fetchall()
    tripList = dict(result)
    
    cursor.execute('SELECT driver_code,alpl_driver_id from alpl_driver_list_master where org_id='+str(org_id))
    dr_result=cursor.fetchall()
    driverid_map = dict(dr_result)
    
    if (source=='MINING_TRIP_DRIVERS'):
        columMap = {
        'alpl_trip_id': 'tripid',
        'driver_id': 'drivercode',
        'in_time': 'intime',
        'out_time': 'outtime',
        'trip_id': ''}
    elif source=='MINING_TRIP_DRILLING':
        columMap = {
        'alpl_trip_id': 'tripid',
        'driver_id': 'drivercode',
        'depth': 'depth',
        'no_of_holes': 'noofholes',
        'grade':'MaterialGrade',
        'trip_id': ''}

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_mining_trip_driver_trans ('
        i = 0
        try:
            for table_column,json_column in columMap.items():
                if i==0:
                    query=query+table_column
                    value = item.get(json_column, None)
                    if (value == None):
                        value = ""

                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset=dataset+validate_string(value,table_column)
                else:
                    query=query+','+table_column
                    value = item.get(json_column, None)
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = str(value)

                    value = validate_string(value, table_column)
                    dataset=dataset+','+value

                i=1
            dataset=dataset+')'
            query=query+')'
            totalQuery=query+dataset
            cursor.execute(totalQuery)
        except Exception as e:
            logger.info(e)
            con.rollback()
            try:
                if (type(q) in (int, float)):
                    q = str(q)
                elif (q == None):
                    q = ""
                if (type(e) in (int, float)):
                    e = str(e)
                elif (e == None):
                    e = ""
                if (type(src) in (int, float)):
                    src = str(src)
                elif (src == None):
                    src = ""
                errorQuery = 'insert into alpl_data_ingestion_error_log(table_name,header_name,record,log) values('
                errorQuery = errorQuery + validate_string('alpl_activity_master', 'tab') + ',' + str(
                    validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + str(validate_string(e, 'tab')) + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                logger.info(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p=p+1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_trip_diesel:", p)
    logger.info("errors :", errors)
    logger.info("connection closed successfully")




def insert_asset_telemetry(unt,asset_map,asset):
    try:
        con = Util.engine.raw_connection()
        cursor=con.cursor()
        cursor.execute('select trip_id from alpl_trip_data where alpl_trip_id=' + unt )
        res=cursor.fetchall()
        trip = res[0][0] if len(res)>0 else None
        if trip!= None:
            cursor.execute('select trip_id from alpl_asset_telemetry where trip_id=' + str(trip) )
            ast = cursor.fetchall()
            if len(ast)==0:
                insert_qry = 'insert into alpl_asset_telemetry(imei,asset_reg_number,trip_id,message_timestamp,status,created_time,last_updated_time) values'
                imei = asset_map.get(asset,None)
                if imei==None:
                    imei='null'
                insert_qry = insert_qry+'('+str(imei)+",'"+asset+"',"+str(trip)+",0,'AT UNIT',now(),now())"
                cursor.execute(insert_qry)
    except Exception as exp:
        logger.error(str(exp))
        con.rollback()
        con.close()
    else:
        con.commit()
        con.close()

def update_asset_telemetry(unt,asset_map,asset):
    try:
        con = Util.engine.raw_connection()
        cursor=con.cursor()
        cursor.execute('select trip_id from alpl_trip_data where alpl_trip_id=' + unt )
        res=cursor.fetchall()
        trip = res[0][0] if len(res)>0 else None
        if trip!= None:
            cursor.execute('select trip_id from alpl_asset_telemetry where trip_id=' + str(trip) )
            ast = cursor.fetchall()
            if len(ast)==0:
                insert_qry = 'insert into alpl_asset_telemetry(imei,asset_reg_number,trip_id,message_timestamp,status,created_time,last_updated_time) values'
                imei = asset_map.get(asset,None)
                if imei==None:
                    imei='null'
                insert_qry = insert_qry+'('+str(imei)+",'"+asset+"',"+str(trip)+",0,'On Road',now(),now())"
                cursor.execute(insert_qry)
    except Exception as exp:
        logger.error(str(exp))
    else:
        con.commit()
        con.close()

       
def main(file_name):  # confirms that the code is under main function
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #file = event
    #file = os.path.abspath('E:/prod_dump') + "/0803/trips.json"
    try:
        global filename
        filename = file_name
        json_obj = Util.getJsonFile(Util.mining_portal_bucket, file_name)
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
            dest_bucket = Util.mining_processed_bucket
        elif (data == 'SOURCE'):
            dataSource = 'DATA'
            dest_bucket = Util.mining_processed_bucket
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
        if source== 'MINING_TRIP_TRANS':
            func = load_trip_trans
        elif source == 'TRIP_ROUTE_TRANS':
            func = load_trip_route_txn
            #stored_proc = "update_trip_route"
        elif source== 'MINING_TRIP_DC_TRANS':
            func=load_trip_dc
        elif source== 'MINING_TRIP_ROUTELEGS_TRANS':
            pass
            func=load_trip_route_leg_trans
            stored_proc='update_leg_status_mining'
        elif source=='MINING_TRIP_DRILLING' or source=='MINING_TRIP_DRIVERS':
            func = load_trip_driver_trans
        if func != None:
            MultiProc.runDataIngestion(jsonData, func,source)
        if stored_proc!=None:
            try:
                Util.call_procedure(stored_proc)
            except Exception as e:
                logger.error("Error calling stored proc:"+stored_proc+ str(e))
        Util.move_blob(Util.mining_portal_bucket, file_name, dest_bucket, file_name)
        Util.call_error_data_url(filename, sourcetype)
    except Exception as ex:
        logger.error("Halting json data load processing due to following exception:" + str(ex))
        Util.move_blob(Util.mining_portal_bucket, file_name, Util.error_bucket, file_name)
        Util.send_alert(file_name, str(ex))