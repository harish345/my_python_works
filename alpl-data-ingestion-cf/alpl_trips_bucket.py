import psycopg2;
import os, json;
import requests;
from datetime import datetime
file = os.path.abspath('D:/json_files') + "/parts_7.json"
json_data = open(file).read()
json_obj = json.loads(json_data)

con = psycopg2.connect(user="postgres",
                       password="admin",
                       host="34.93.62.36",
                       port="5432",
                       database="alpl_integration")

cursor = con.cursor()
cursor.execute('SELECT alpl_trip_id FROM alpl_dev_redesign.alpl_trip_data')
result = cursor.fetchall()
tripList = [i[0] for i in result]



# cursor.execute('SELECT dc_no from alpl_dev_redesign.alpl_dc_details')
# result = cursor.fetchall()
# dcNoList = [i[0] for i in result]

# cursor.execute('SELECT route_path_id,route_description FROM alpl_dev_redesign.alpl_route_master')
# result = cursor.fetchall()
# pathList = [i[0] for i in result]
# routeDesList = [i[1] for i in result]
# routePathIdList = pathList

cursor.execute('SELECT asset_reg_number from alpl_dev_redesign.alpl_asset_master')
result = cursor.fetchall()
regList = [i[0] for i in result]

orderQuery = "SELECT order_id FROM alpl_dev_redesign.alpl_orders"
cursor.execute(orderQuery)
result = cursor.fetchall()
orderList = [i[0] for i in result]


def validate_string(val, val1):

    if val.replace('.', '', 1).isdigit() and val1 != 'int_indent_date':
        if val != None and val != "":
            return val
        else:
            return 'NULL'
    elif((val.find("'", 0, len(val)) >= 0)):
        pos = -1
        pos = val.find("'", 0, len(val))
        if(pos >=0):
            val = val[:pos]+'\''+val[pos:]
            return 'E'+"'" + val + "'"
        return "'" + val + "'"
    if val1 == 'trip_creation_time' or val1 == 'trip_close_time':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val1 == 'bll_date' or val1 == 'dfd_created_date' or val1 == 'dc_date' or val1 == 'effective_from' or val1 == 'effective_to':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val1 == 'asset_reg_date' or val1 == 'asset_valid_till' or val1 == 'created_time' or val1 == 'asset_modf_date' or val1 == 'asset_dec_date' or val1 == 'pol_certificate_ldate' or val1 == 'pol_certificate_end_date' or val1 == 'nat_permit_end_date' or val1 == 'ins_expiry' or val1 == 'fit_certifcate_expiry_date' or val1 == 'ad_blue_fdate':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'

    elif val1 == 'order_created_date':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'

    elif val1 == 'order_completion_date':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'

    elif val1 == 'ext_indent_date' or val1 == 'int_indent_date':
        if (val == '0000-00-00') or (val == 'None'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'

    elif val != None and val != "":
        return "'" + val + "'"
    else:
        return 'NULL'

cursor.execute('SELECT alpl_emp_id,emp_id FROM alpl_dev_redesign.alpl_employee_master')
result = cursor.fetchall()
alplEmpIdList = [i[0] for i in result]
empIdList = [i[1] for i in result]

dictEmpID={}
length = len(alplEmpIdList)
for x in range(length):
    dictEmpID[alplEmpIdList[x]] = empIdList[x]


cursor.execute('SELECT route_id,CONCAT(from_location,to_location) FROM alpl_dev_redesign.alpl_route_master')
result = cursor.fetchall()
routeIdList = [i[0] for i in result]
routeLocList = [i[1] for i in result]

dictRouteIdLoc={}
length = len(routeIdList)
for x in range(length):
    dictRouteIdLoc[routeLocList[x]] = routeIdList[x]



cursor.execute('SELECT unit_district_id,upper(district) FROM alpl_dev_redesign.alpl_unit_district_mapping')
result = cursor.fetchall()
districtId = [i[0] for i in result]
districtName = [i[1] for i in result]

dictDistrict={}
length = len(districtId)
for x in range(length):
    dictDistrict[districtName[x]] = districtId[x]

def getDistrictId(district):
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

def getEmpId(alplEmpId):
    if(alplEmpId == None):
        value = ""
    else:
        value = dictEmpID.get(int(alplEmpId))

    if(value == None):
        value = ""
    elif(type(value) in (int, float)):
        value = str(value)
    return value

def getRouteId(fromLoc,ToLoc):
    val = fromLoc + ToLoc
    value = dictRouteIdLoc.get(val)


    if (value == None):
        value = ""
    elif (type(value) in (int, float)):
        value = str(value)
    return value

def vigilence_trip_trans(bucketname, filename_with_path):
    print("Data ingestion starting for alpl_trip_data")
    #json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    q = None
    ctu = 0
    cti = 0
    errors = 0
    src = 'logistics'
    # if ("MINING" in json_obj["Source"]):
    #     src = 'mining'
    columMap = {
                'alpl_trip_id': 'TripID',
                'asset_reg_number': 'VehRegNo',
                'driver_id': 'DriverID',
                'route_from': 'RouteFrom',
                'route_to': 'RouteTo',
                'is_completed': 'IsCompleted',
                'trip_creation_time': 'CreatedDateTime',
                'vigilance_officer_id': 'CreatedUserID',
                'trip_close_time': 'ClosedDateTime',
                'advance_amount': 'AdvanceAmount',
                'ad_blue': 'AdBlueQuantity',
                'recovery': 'RecoveryAmount',
                'exemption_amount': 'ExemptionAmount',
                'budget': 'Budget',
                'actual_expense': 'ActualExpenses',
                'target_kmpl': 'TargetKmpl',
                'actual_kmpl': 'ActualKmpl',
                'actual_kms': 'ActualKms',
                'target_liters': 'TargetDieselQuantity',
                'master_route_id': '',
                'trip_id' : ''}
    tripIdMax = 0
    cursor.execute('select max(trip_id) FROM alpl_dev_redesign.alpl_trip_data')
    result = cursor.fetchall()[0]
    if (result[0] == None):
        tripIdMax = 0
    else:
        tripIdMax = result[0]

    for i, item in enumerate(json_obj["data"]):
        print("______________________________")
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_trip_data ('
        updateQuery = 'UPDATE alpl_dev_redesign.alpl_trip_data SET '
        i = 0
        flag = 0
        valueID = '0'
        fromLocation = None
        toLocation = None

        try:
            for table_column, json_column in columMap.items():

                if (flag == 1) or (table_column == 'alpl_trip_id' and (
                        int(validate_string(item.get(json_column, None), table_column)) in tripList)):
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
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    else:
                        if (table_column != 'trip_id'):
                            updateQuery = updateQuery + ',' + table_column

                        value = item.get(json_column, None)

                        if (table_column == 'vigilance_officer_id'):
                            value = getEmpId(value)
                            # empIdQuery = 'SELECT emp_id FROM alpl_dev_redesign.alpl_employee_master WHERE alpl_emp_id = '
                            # if (value == None):
                            #     value = ""
                            #
                            # else:
                            #     empIdQuery = empIdQuery + '\'' + value + '\''
                            #     cursor.execute(empIdQuery)
                            #     empId = cursor.fetchone()
                            #     if (empId == None):
                            #         value = ""
                            #     else:
                            #         value = empId[0]

                        if (table_column == 'is_completed'):
                            if (value == 'True'):
                                value = 1
                            elif (value == 'False'):
                                value = 0
                        if (table_column == 'route_from'):
                            fromLocation = str(item.get(json_column, None))

                        if (table_column == 'route_to'):
                            toLocation = str(item.get(json_column, None))



                        if (table_column == 'master_route_id'):
                            value = getRouteId(fromLocation,toLocation)
                            print("route")
                            print(value)
                            # routeIdQuery = 'SELECT route_id FROM alpl_dev_redesign.alpl_route_master WHERE lower(from_location) = lower(' + '\'' + fromLocation + '\')' + ' and '
                            # routeIdQuery = routeIdQuery + 'lower(to_location) = lower(' + '\'' + toLocation + '\')'
                            # cursor.execute(routeIdQuery)
                            # routeId = cursor.fetchone()
                            # if (routeId == None):
                            #     value = ""
                            # else:
                            #     value = routeId[0]

                        if (type(value) in (int, float)):
                            value = str(value)

                        if (value == None):
                            value = ""
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


                        if (table_column == 'trip_id'):
                            tripIdMax = tripIdMax + 1
                            value = tripIdMax

                        if (table_column == 'vigilance_officer_id'):
                            value = getEmpId(value)
                            # empIdQuery = 'SELECT emp_id FROM alpl_dev_redesign.alpl_employee_master WHERE alpl_emp_id = '
                            # if (value == None):
                            #     value = ""
                            # else:
                            #     empIdQuery = empIdQuery + '\'' + value + '\''
                            #     cursor.execute(empIdQuery)
                            #     empId = cursor.fetchone()
                            #     if (empId == None):
                            #         value = ""
                            #     else:
                            #         value = empId[0]

                        if (table_column == 'is_completed'):
                            if (value == 'True'):
                                value = 1
                            elif (value == 'False'):
                                value = 0

                        if (table_column == 'route_from'):
                            fromLocation = str(item.get(json_column, None))

                        if (table_column == 'route_to'):
                            toLocation = str(item.get(json_column, None))

                        if (table_column == 'master_route_id'):
                            value = getRouteId(fromLocation, toLocation)
                            # routeIdQuery = 'SELECT route_id FROM alpl_dev_redesign.alpl_route_master WHERE from_location = ' + '\'' + fromLocation + '\'' + ' and '
                            # routeIdQuery = routeIdQuery + 'to_location = ' + '\'' + toLocation + '\''
                            # cursor.execute(routeIdQuery)
                            # routeId = cursor.fetchone()
                            # if (routeId == None):
                            #     value = ""
                            # else:
                            #     value = routeId[0]

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where alpl_trip_id=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                print(q)
                #cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                print(q)
                #cursor.execute(totalQuery)
                cti = cti + 1
        except Exception as e:
            print(e, i)
            print(q)
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
                errorQuery = errorQuery + validate_string('alpl_vigilence_trip_data', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1

        else:
            con.commit()
            p = p + 1

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_vigilence_trip_data:", p)
    print("number of updated rows :",ctu)
    print("number of inserted rows :",cti)
    print("errors :", errors)
    print("connection closed successfully")

def trip_trans(bucketname, filename_with_path):
    print("Data ingestion starting for alpl_trip_data")
    #json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    q = None
    ctu = 0
    cti = 0
    src = 'logistics'
    errors = 0
    if ("MINING" in json_obj["Source"]):
        src = 'mining'

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
                 'int_order_id': '',
                 'int_indent_issued': '',
                 'int_indent_date': '',
                 'int_price': '',
                 'ext_order_id': '',
                 'ext_indent_issued': '',
                 'ext_indent_date': '',
                 'ext_price': '',
                 'master_route_id': '',
                #'unit_id':'',
                 'trip_id' : ''
                  }
    tripIdMax = 0
    cursor.execute('select max(trip_id) FROM alpl_dev_redesign.alpl_trip_data')
    result = cursor.fetchall()[0]
    if (result[0] == None):
        tripIdMax = 0
    else:
        tripIdMax = result[0]

    for i, item in enumerate(json_obj["data"]):
        #print('_________________________________________')
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_trip_data ('
        updateQuery = 'UPDATE alpl_dev_redesign.alpl_trip_data SET '
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
        try:
            for table_column, json_column in columMap.items():
                #print('----------------------------------')
                if((table_column == 'alpl_trip_id' and (
                        int(validate_string(item.get(json_column, None), table_column)) in tripList))):
                    isCompletedQuery = 'select trip_id from alpl_trip_data where is_completed = 0 and alpl_trip_id = '+ str(int(item.get(json_column, None)))
                    # print(isCompletedQuery)print(isCompletedQuery)
                    cursor.execute(isCompletedQuery)
                    result = cursor.fetchall()
                    if(len(result) != 0):
                        serialTripId = result[0][0]
                        updateFlag = 1
                        if (type(serialTripId) in (int, float)):
                            serialTripId = str(serialTripId)
                        if (serialTripId == None):
                            serialTripId = ""




                if (flag == 1) or (table_column == 'alpl_trip_id' and updateFlag == 1):
                    if flag == 0:
                        value = item.get(json_column, None)
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        valueID = validate_string(serialTripId, table_column)
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
                        if (table_column != 'trip_id'):
                            updateQuery = updateQuery + ',' + table_column

                        value = item.get(json_column, None)

                        # if (table_column == 'vigilance_officer_id'):
                        #     value = getEmpId(value)
                            # empIdQuery = 'SELECT emp_id FROM alpl_dev_redesign.alpl_employee_master WHERE alpl_emp_id = '
                            # if (value == None):
                            #     value = ""
                            #
                            # else:
                            #     empIdQuery = empIdQuery + '\'' + value + '\''
                            #     cursor.execute(empIdQuery)
                            #     empId = cursor.fetchone()
                            #     if (empId == None):
                            #         value = ""
                            #     else:
                            #         value = empId[0]

                        if (table_column == 'is_completed'):
                            if (value == 'True'):
                                value = 1
                                workFlowFlag = 1
                            elif (value == 'False'):
                                value = 0
                        if (table_column == 'route_from'):
                            fromLocation = str(item.get(json_column, None))

                        if (table_column == 'route_to'):
                            toLocation = str(item.get(json_column, None))




                        if (table_column == 'master_route_id'):
                            value = getRouteId(fromLocation,toLocation)
                            # routeIdQuery = 'SELECT route_id FROM alpl_dev_redesign.alpl_route_master WHERE lower(from_location) = lower(' + '\'' + fromLocation + '\')' + ' and '
                            # routeIdQuery = routeIdQuery + 'lower(to_location) = lower(' + '\'' + toLocation + '\')'
                            # cursor.execute(routeIdQuery)
                            # routeId = cursor.fetchone()
                            # if (routeId == None):
                            #     value = ""
                            # else:
                            #     value = routeId[0]

                        if (table_column == 'int_order_id'):
                            internalQuery = 'select order_id,indent_issued,indent_issued_date,indent_price'
                            internalQuery = internalQuery + " from alpl_diesel_indent_internal where vehicle_number = " + str(veh_reg)
                            internalQuery = internalQuery + " and indent_issued_date >= " + str(trip_cr_date) + " and indent_issued_date <= " + str(trip_ed_date)
                            internalQuery = internalQuery + " order by indent_issued_date desc limit 1"
                            cursor.execute(internalQuery)
                            records = cursor.fetchall()
                            row = len(records)

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
                            #print(externalQuery)
                            cursor.execute(externalQuery)
                            records = cursor.fetchall()
                            row = len(records)

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


                        if (table_column == 'trip_id'):
                            tripIdMax = tripIdMax + 1
                            value = tripIdMax

                        # if (table_column == 'vigilance_officer_id'):
                        #     value = getEmpId(value)
                            # empIdQuery = 'SELECT emp_id FROM alpl_dev_redesign.alpl_employee_master WHERE alpl_emp_id = '
                            # if (value == None):
                            #     value = ""
                            # else:
                            #     empIdQuery = empIdQuery + '\'' + value + '\''
                            #     cursor.execute(empIdQuery)
                            #     empId = cursor.fetchone()
                            #     if (empId == None):
                            #         value = ""
                            #     else:
                            #         value = empId[0]

                        if (table_column == 'is_completed'):
                            if (value == 'True'):
                                value = 1
                                workFlowFlag = 1
                            elif (value == 'False'):
                                value = 0

                        if (table_column == 'route_from'):
                            fromLocation = str(item.get(json_column, None))

                        if (table_column == 'route_to'):
                            toLocation = str(item.get(json_column, None))

                        if (table_column == 'master_route_id'):
                            value = getRouteId(fromLocation, toLocation)

                        if(table_column == 'int_order_id'):
                            internalQuery = 'select order_id,indent_issued,indent_issued_date,indent_price'
                            internalQuery = internalQuery + " from alpl_diesel_indent_internal where vehicle_number = "+str(veh_reg)
                            internalQuery = internalQuery + " and indent_issued_date >= " + str(trip_cr_date)+" and indent_issued_date <= "+ str(trip_ed_date)
                            internalQuery = internalQuery + " order by indent_issued_date desc limit 1"
                            #print(internalQuery)
                            cursor.execute(internalQuery)
                            records = cursor.fetchall()
                            row = len(records)

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
                            #print(externalQuery)
                            cursor.execute(externalQuery)
                            records = cursor.fetchall()
                            row = len(records)

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



                            # routeIdQuery = 'SELECT route_id FROM alpl_dev_redesign.alpl_route_master WHERE from_location = ' + '\'' + fromLocation + '\'' + ' and '
                            # routeIdQuery = routeIdQuery + 'to_location = ' + '\'' + toLocation + '\''
                            # cursor.execute(routeIdQuery)
                            # routeId = cursor.fetchone()
                            # if (routeId == None):
                            #     value = ""
                            # else:
                            #     value = routeId[0]

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
            updateQuery = updateQuery + ' where trip_id=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                #print(q)
                cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                #print(q)
                cursor.execute(totalQuery)
                cti = cti + 1
        except Exception as e:
            print(e, i)
            print(q)
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
                errorQuery = errorQuery + validate_string('alpl_trip_data', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1

        else:
            con.commit()
            # if (workFlowFlag == 1):
            #     workFlowData['assetRegNumber'] = str(veh_reg)
            #     unt = int(unt[0:4])
            #     workFlowData['unit'] = unt
            #     auth_token = 'd087db24-d28f-4998-99e9-63f9e5ff4ac2'
            #     hed = {'Authorization': 'Bearer ' + auth_token}
            #     requests.post("http://localhost:3000/lifecycle/start", params=workFlowData, headers=hed)
            p = p + 1

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_trip_data:", p)
    print("number of updated rows :",ctu)
    print("number of inserted rows :",cti)
    print("errors :", errors)
    print("connection closed successfully")



def trip_dc(bucketname, filename_with_path):
    print("Data ingestion starting for alpl_trip_dc")
    #json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    ctu = 0
    cti =0
    wst = 0
    dc_id = 0
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj["Source"]):
        src = 'mining'
    columMap = {'dc_no': 'DCNo',
                'trip_id': 'TripID',
                'vehicle_reg_no': 'VehRegNo',
                'alpl_driver_id': 'DriverID',
                'order_id': 'OrderID',
                'quantity': 'Quantity',
                'dc_date': 'dcdate',
                'incentive_amount': 'IncentiveAmount',
                'dc_type_id':'DcTypeID',
                'unit_id':'LocationID'}

    IdQuery = 'SELECT max(dc_details_id) FROM alpl_dev_redesign.alpl_dc_details'
    cursor.execute(IdQuery)
    id = cursor.fetchone()[0]

    if(id == None):
        dc_id = 0
    else:
        dc_id = id

    for i, item in enumerate(json_obj["data"]):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_dc_details ('
        #updateQuery = 'UPDATE alpl_dev_redesign.alpl_dc_details SET '
        i = 0
        #flag = 0
        valueID = '0'
        dc_id = dc_id + 1
        try:
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

                    if (table_column == 'unit_id'):
                        if(value == '1'):
                            value = 8
                        elif(value == '2'):
                            value = 7

                    if (table_column == 'trip_id'):
                        tripIdQuery = 'SELECT trip_id FROM alpl_dev_redesign.alpl_trip_data WHERE alpl_trip_id = '
                        temp = item.get(json_column, None)
                        if (temp == None):
                            value = ""
                        else:
                            tripIdQuery = tripIdQuery + temp
                            cursor.execute(tripIdQuery)
                            tripId = cursor.fetchone()
                            if (tripId == None):
                                value = ""
                            else:
                                value = tripId[0]

                    # if (table_column == 'alpl_driver_id'):
                    #     driverIdQuery = 'SELECT driver_list_master_id FROM alpl_dev_redesign.alpl_driver_list_master WHERE alpl_driver_id = '
                    #     temp = item.get(json_column, None)
                    #     if (temp == None):
                    #         value = ""
                    #     else:
                    #         driverIdQuery = driverIdQuery + temp
                    #         cursor.execute(driverIdQuery)
                    #         driverId = cursor.fetchone()
                    #         if (driverId == None):
                    #             value = ""
                    #         else:
                    #             value = driverId[0]

                    if (table_column == 'dc_type_id'):
                        if (value==0):
                            wst = wst + 1

                    if (type(value) in (int, float)):
                        value = str(value)
                    elif (value == None):
                        value = ""

                    dataset = dataset + ',' + validate_string(value, table_column)
                i = 1
            dataset = dataset + ','+str(dc_id)+')'
            query = query + ',dc_details_id)'
            #updateQuery = updateQuery + ' where dc_no=' + valueID
            totalQuery = query + dataset

            #print(totalQuery)
            cursor.execute(totalQuery)
            cti = cti+1
        except Exception as e:
            print(e, i)
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
                errorQuery = errorQuery + validate_string('alpl_trip_dc', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_trip_dc:", p)
    print("number of updated rows :",ctu)
    print("number of inserted rows :",cti)
    print("count of dc_type_id with 0 : ",wst)
    print("errors :", errors)
    print("connection closed successfully")

def load_order_master(bucketname, filename_with_path):
    print("Data ingestion starting for alpl_order_master")
    #json_obj = getJsonFile(bucketname, filename_with_path)
    orgID = 1
    src = 'logistics'
    if ("MINING" in json_obj["Source"]):
        orgID = 2
        src = 'mining'

    cursor = con.cursor()
    cursor.execute('SELECT alpl_reference_id,alpl_location_master_id FROM alpl_dev_redesign.alpl_location_master where org_id = '+str(orgID))
    result = cursor.fetchall()
    AlplLocId = [i[0] for i in result]
    LocMastId = [i[1] for i in result]

    dictLoc = {}
    length = len(AlplLocId)
    for x in range(length):
        dictLoc[AlplLocId[x]] = LocMastId[x]

    p = 0

    alpl_orders_id = None
    ucnt = 0
    icnt = 0
    errors = 0
    columMap = {'order_id': 'ALPL Reference ID',
                'order_created_date' : 'createddatetime',
                'quantity': 'Quantity',
                'order_status': 'Order Status',
                'order_destination_type' : 'Order Destination Type(Godown/Dealer)',
                'order_completion_date' : 'CompletionDateTime',
                'dealer_details' : 'Dealer Details',
                'grade' : 'Grade',
                'consignee_district': 'ConsigneeDistrict',
                'org_id':'',
                'from_location_id':'FromLocationID',
                'order_type':'OrderTypeID',
                'frieght_charge':'FrieghtCharge',
                'billing_cust_location_id':'BillingCustLocationID'
                #'district': 'ConsigneeDistrict'
                }
    cursor.execute('SELECT max(alpl_orders_id) from alpl_dev_redesign.alpl_orders')
    result = cursor.fetchall()[0]
    if (result[0] == None):
        alpl_orders_id = 0
    else:
        alpl_orders_id = result[0]
    for i, item in enumerate(json_obj["data"]):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_orders('
        updateQuery = 'UPDATE alpl_dev_redesign.alpl_orders SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None

        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or ((table_column=='order_id') and (item.get(json_column, None) in orderList)):
                    if (flag == 0):
                        value = item.get(json_column, None)
                        if (i == 0):
                            updateQuery = updateQuery + table_column

                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        valueID = item.get(json_column, None)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                        flag = 1
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if(table_column == 'consignee_district'):
                            value = getDistrictId(value)

                        if(table_column == 'org_id'):
                            value = orgID
                        if(table_column == 'from_location_id'):
                            value = dictLoc.get(int(value))

                        if(table_column == 'billing_cust_location_id'):
                            value = dictLoc.get(int(value))

                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    i = 1

                else:
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)
                        alpl_orders_id = alpl_orders_id + 1
                        if (value == None) :
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset +  validate_string(value, table_column)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'consignee_district'):
                            value = getDistrictId(value)

                        if (table_column == 'org_id'):
                            value = orgID

                        if (table_column == 'from_location_id'):
                            value = dictLoc.get(int(value))

                        if (table_column == 'billing_cust_location_id'):
                            value = dictLoc.get(int(value))

                        if (value == None) :
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1

            dataset = dataset +"," +str(alpl_orders_id)+')'
            query = query +  ',alpl_orders_id)'
            updateQuery = updateQuery + ' where order_id = '+"'"+str(valueID)+"'"
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                #print(q)
                cursor.execute(updateQuery)
                ucnt = ucnt + 1
            else:
                q = totalQuery
                #print(q)
                cursor.execute(totalQuery)
                icnt = icnt + 1
        except Exception as e:

            print(e, i)
            print(q)
            print(dataset)
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
                errorQuery = errorQuery + validate_string('alpl_orders_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            #print("data inserted successfully")
            con.commit()
            p = p + 1
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_orders_master:", p)
    print("connection closed successfully")
    print("inserted rows :",icnt)
    print("updated rows :",ucnt)
    print("errors :", errors)


def route_master(bucketname, filename_with_path):
    print("Data ingestion starting for alpl_route_master")
    # json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    q = None
    ctu = 0
    cti = 0
    routeId = 0
    if ("MINING" in json_obj["Source"]):
        columMap = {'route_path_id': 'RoutePathID', 'alpl_route_id': 'RouteID', 'from_location': 'Unit',
                    'to_location': 'District', 'route_description': 'RouteDescription', 'pit_id': 'PITID',
                    'bench_id': 'BenchID'}

    else:
        columMap = {'route_path_id': 'RoutePathID', 'alpl_route_id': 'RouteID', 'from_location': 'Unit',
                    'to_location': 'District', 'route_description': 'RouteDescription'}

    p = 0

    # cursor.execute('SELECT max(route_id) from alpl_dev_redesign.alpl_route_master')
    # result = cursor.fetchall()[0]
    if (result[0] == None):
        routeId = 0
    else:
        routeId = result[0]

    for i, item in enumerate(json_obj["data"]):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_route_master ('
        updateQuery = 'UPDATE alpl_dev_redesign.alpl_route_master SET '
        i = 0
        flag = 0
        valueID = '0'
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'route_path_id' and (
                        int(validate_string(item.get(json_column, None), table_column)) in pathList)):
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
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        # if (table_column == 'unit_code'):
                        #     unitIdQuery = 'SELECT unit_id FROM alpl_dev_redesign.alpl_unit_master WHERE unit_code = '
                        #
                        #     if (value == None):
                        #         value = ""
                        #     else:
                        #         unitIdQuery = unitIdQuery + '\'' + str(value) + '\''
                        #         cursor.execute(unitIdQuery)
                        #         unitId = cursor.fetchone()
                        #         if (unitId == None):
                        #             value = ""
                        #         else:
                        #             value = unitId[0]

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

                        # if (table_column == 'unit_code'):
                        #     unitIdQuery = 'SELECT unit_id FROM alpl_dev_redesign.alpl_unit_master WHERE unit_code = '
                        #     if (value == None):
                        #         value = ""
                        #     else:
                        #         unitIdQuery = unitIdQuery + '\'' + str(value) + '\''
                        #         cursor.execute(unitIdQuery)
                        #         unitId = cursor.fetchone()
                        #         if (unitId == None):
                        #             value = ""
                        #         else:
                        #             value = unitId[0]
                        #     if (type(value) in (int, float)):
                        #         value = str(value)
                        #     if (value == None):
                        #         value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1
            # routeId = routeId + 1
            # dataset = dataset +' , '+ str(routeId) +',2)'
            dataset = dataset + ' , ' + str(2) + ')'
            query = query + ',org_id)'
            updateQuery = updateQuery + ' where route_path_id=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                # print(updateQuery)
                cursor.execute(updateQuery)
                q = updateQuery
                ctu = ctu + 1
                # print(q)
            else:
                # print(totalQuery)
                cursor.execute(totalQuery)
                q = totalQuery
                cti = cti + 1
                # print(q)
        except Exception as e:
            print(e)
            con.rollback()
        else:
            p = p + 1
            con.commit()

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_route_master:", p)
    print("connection closed successfully")
    print("inserted :", cti)
    print("updated:", ctu)

def trip_route_trans(bucketname, filename_with_path):
    print("Data ingestion starting for alpl_trip_route_trans")
    #json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    src = 'logistics'
    if ("MINING" in json_obj["Source"]):
        src = 'mining'

    columMap = {'trip_id': 'TripID',
                'route_path_id': 'RoutePathID'}
    for i, item in enumerate(json_obj["data"]):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_trip_route_trans ('
        i = 0
        flag = 0
        valueID = '0'
        q = None
        errors = 0
        try:
            for table_column, json_column in columMap.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'trip_id'):
                        tripIdQuery = 'SELECT trip_id FROM alpl_dev_redesign.alpl_trip_data WHERE alpl_trip_id = '
                        if (value == None):
                            value = ""
                        else:
                            tripIdQuery = tripIdQuery + '\'' + value + '\''
                            cursor.execute(tripIdQuery)
                            tripId = cursor.fetchone()
                            if (tripId == None):
                                value = ""
                            else:
                                value = tripId[0]

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + validate_string(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)

                    if (table_column == 'route_path_id'):
                        routeIdQuery = 'SELECT route_id FROM alpl_dev_redesign.alpl_route_master WHERE route_path_id = '
                        if (value == None):
                            value = ""
                        else:
                            routeIdQuery = routeIdQuery + '\'' + value + '\''
                            cursor.execute(routeIdQuery)
                            pathId = cursor.fetchone()
                            if (pathId == None):
                                value = ""
                            else:
                                value = pathId[0]

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string(value, table_column)
                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset

            q = totalQuery
            cursor.execute(totalQuery)
        except Exception as e:
            print(e, i)
            print(q)
            print(dataset)
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
                errorQuery = errorQuery + validate_string('alpl_trip_route_trans', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_trip_route_trans:", p)
    print("errors:",errors)
    print("connection closed successfully")

def trip_diesel(bucketname, filename_with_path):
    print("Data ingestion starting for alpl_trip_diesel")
    #json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    errors = 0
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

    for i, item in enumerate(json_obj["data"]):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_diesel_details ('
        i = 0
        IdQuery = "select max(diesel_detail_id) from alpl_dev_redesign.alpl_diesel_details"
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
            print(e)
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
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p=p+1
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_trip_diesel:", p)
    print("errors :", errors)
    print("connection closed successfully")

trip_route_trans('x','y')
