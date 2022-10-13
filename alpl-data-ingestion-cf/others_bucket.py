import psycopg2;
import os, json;
import re;
import multiprocessing as mp;
import time;
from multiprocessing import Process;
from datetime import datetime;
import calendar;

# read JSON file which is in the next parent folder
file = os.path.abspath('D:/json_files') + "/sch.json"
json_data = open(file, encoding='utf-8', errors='ignore').read()
json_obj = json.loads(json_data)
legList = []
locationIdList = []
locationTypeIdList=[]
vendorIdList=[]
regList=[]
trips={}
shifts={}
dictUnitc={}
dictState={}
legList,locationIdList,locationTypeIdList,vendorIdList,regList,trips,shifts,dictUnitc,dictState
con = psycopg2.connect(user="postgres",
                       password="admin",
                       host="34.93.62.36",
                       port="5432",
                       database="alpl_test")
cursor = con.cursor()

cursor = con.cursor()
cursor.execute('SELECT alpl_leg_id from alpl_uat.alpl_leg_master')
result = cursor.fetchall()
legList = [i[0] for i in result]

cursor.execute('SELECT alpl_location_master_id from alpl_uat.alpl_location_master')
result=cursor.fetchall()
locationIdList=[i[0] for i in result]

cursor.execute('SELECT alpl_location_type_id from alpl_uat.alpl_location_type_master')
result=cursor.fetchall()
locationTypeIdList=[i[0] for i in result]

cursor.execute('SELECT alpl_supplier_id from alpl_uat.alpl_vendor_master')
result=cursor.fetchall()
vendorIdList=[i[0] for i in result]

cursor.execute('SELECT asset_reg_number from alpl_uat.alpl_asset_master')
result = cursor.fetchall()
regList = [i[0] for i in result]

cursor.execute('SELECT upper(concat(shift,br_shift)),shift_id from alpl_uat.alpl_shift_master')
result = cursor.fetchall()
shiftCodeList = [i[0] for i in result]
shiftIdList = [i[1] for i in result]


length = len(shiftIdList)
for x in range(length):
    shifts[shiftCodeList[x]] = shiftIdList[x]

cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_uat.alpl_unit_master')
result = cursor.fetchall()
unitCodeList = [i[0] for i in result]
unitIdList = [i[1] for i in result]


length = len(unitCodeList)
for x in range(length):
    dictUnitc[unitCodeList[x]] = unitIdList[x]


def getUnitId(unit):
    if(unit == 'ALPL'):
        return 0
    return dictUnitc.get(int(unit))



cursor.execute('SELECT alpl_state_id,state_master_id FROM alpl_uat.alpl_state_master')
result = cursor.fetchall()
alplStateIdList = [i[0] for i in result]
stateIdList = [i[1] for i in result]


length = len(alplStateIdList)
for x in range(length):
    dictState[alplStateIdList[x]] = stateIdList[x]


def getStateId(state):
    return dictState.get(int(state))


def getShiftId(shift,shiftCode):
    shiftCombine = str(shift) + str(shiftCode)
    shiftCombine = shiftCombine.upper()
    return shifts.get(str(shiftCombine))

def validate_string_vendor_location(val,val1,org):
    if val1=='location_type_id':
        query='select alpl_location_type_id from alpl_location_type_master where alpl_reference_id='+val+' and org_id='+str(org)
        cursor.execute(query)
        result=cursor.fetchall()
        return str(result[0][0])
    elif val1=='from_location' or val1=='to_location':
        q="select alpl_location_master_id from alpl_location_master where alpl_reference_id="+val+" and org_id="+str(org)
        cursor.execute(q)
        result=cursor.fetchall()
        print(result[0][0])
        return str(result[0][0])
    elif val.replace('.', '', 1).isdigit():
        if val != None and val != "":
            return val
        else:
            return 'NULL'

    elif ((val.find("'", 0, len(val)) >= 0)):
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
    if val.replace('.', '', 1).isdigit():
        if val != None and val != "":
            return val
        else:
            return 'NULL'

    if ((val.find("'", 0, len(val)) >= 0)):
        val = val.replace('\'', '\\\'')
        return 'E' + "'" + val + "'"




    elif val1 == 'created_date_time' or val1 == 'stock_date_time':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y/%m/%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'


    elif val1 == 'asset_reg_date' or val1 == 'asset_valid_till' or val1 == 'created_time' or val1 == 'asset_modf_date' or val1 == 'asset_dec_date' or val1 == 'expiry_date' or val1 == 'purchase_date' or val1 == 'decomiss_date' or val1 == 'fitment_date' or val1 == 'date' or val1 == 'grn_date' or val1 == 'indent_issued_date':

        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'

    elif val1 == 'tyre_purchase_date' or val1 == 'tyre_war_valid_date' or val1 == 'tyre_scrap_date' or val1 == 'rdate' or val1 == 'valid_to' or val1 == 'valid_from' or val1 == 'startDate':
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


def validate_string_mining(val, val1):
    if val.replace('.', '', 1).isdigit():
        if val != None and val != "":
            return val
        else:
            return 'NULL'

    elif val1 == 'asset_reg_date' or val1 == 'asset_valid_till' or val1 == 'asset_created_on' or val1 == 'asset_modf_date' or val1 == 'effective_from_date' :

        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %I:%M:%S %p') + "'"
        else:
            return 'NULL'

    elif val1 == 'effective_to_date':
        if (val == '00/00/0000'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %I:%M:%S %p') + "'"
        else:
            return 'NULL'


    # elif val == 'NULL':
    #     return 'NULL'
    elif val != None and val != "" and val != 'NULL':
        return "'" + val + "'"

    else:
        return 'NULL'

def load_asset_master(json,source):
    print("Data ingestion starting for asset_master")
    columMap = {
        'asset_reg_number': 'TRUCK_NO',
        #'refid': 'ALPLREFID',
        #'asset_capacity': 'CAPACITY',
        #'asset_type': 'TYPE', [harish asked to remove becoz type= v is vigilence]
        'asset_model_id': 'TRUCK_MODEL',
        'asset_reg_date': 'REG_DATE',
        'asset_valid_till': 'VALID_TILL',
        'asset_smt_cardid': 'SMT_CARDID',
        'asset_status': 'STATUS',
        'asset_created_on': 'CREATED_ON',
        'asset_modf_date': 'MODF_DATE',
        'asset_modf_on': 'MODF_ON',
        'no_of_tyre': 'NOF_TYRE',
        'status_a_i': 'STATUS_A_I',
        'no_of_spare_tyre': 'NO_SPA_TYRE',
        'ad_blue_cap': 'AD_BLUE_CAP',
        'map_tyre': 'MAP_TYRE',
        'asset_dec_date': 'TRUCK_DEC_DATE',
        'asset_unit': 'UNIT',
        'asset_manufacturer':'MANUFACTURER',
        'asset_name':'TRUCK_NAME',
        'usage_ind':'USAGE_IND',
        'COMMERCIAL_NONCommercial':'USAGE_IND_TEXT',
        'temp_check':'',
        'insurance_no':'POLNUM',
        'insurance_name':'INS_PROVIDER',
        'valid_to':'VALID_TO',
        'valid_from':'VALID_FROM',
        'tonnage':''
    }

    cursor = con.cursor()
    cursor.execute('SELECT asset_reg_number from alpl_uat.alpl_asset_master')
    result = cursor.fetchall()
    regList = [i[0] for i in result]

    cursor.execute('SELECT alpl_asset_model_id,asset_model_id FROM alpl_uat.alpl_asset_model')
    result = cursor.fetchall()
    assetModelList = [i[0] for i in result]
    assetModelIdList = [i[1] for i in result]

    dictassetmodel = {}
    length = len(assetModelList)
    for x in range(length):
        dictassetmodel[assetModelList[x]] = assetModelIdList[x]

    cursor.execute('SELECT alpl_asset_model_id,max_load_capacity FROM alpl_uat.alpl_asset_model')
    result = cursor.fetchall()
    modelList = [i[0] for i in result]
    tonnageList = [i[1] for i in result]

    dictassetTonnage = {}
    length = len(modelList)
    for x in range(length):
        dictassetTonnage[modelList[x]] = tonnageList[x]

    ctu = 0
    cti= 0
    p = 0
    errors = 0
    src = 'logistics'
    orgId = 1
    if ("MINING" in json_obj[source]):
        src = 'mining'
        orgId = 2

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_asset_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_asset_master SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        assetModelId = None
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'asset_reg_number' and (
                        item.get(json_column, None) in regList)):
                    if flag == 0:
                        val = item.get(json_column, None)
                        if (type(val) in (int, float)):
                            val = str(val)
                        if (val == None):
                            val = ""

                        valueID = validate_string(val, table_column)
                        flag = 1
                    if (i == 0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)

                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'asset_unit'):
                            value = getUnitId(value)


                        if(table_column == 'temp_check'):
                            value = 1

                        if (table_column == 'asset_model_id'):
                            assetModelId = value
                            value = dictassetmodel.get(int(value))
                            # assetModelIdQuery = 'SELECT asset_model_id FROM alpl_uat.alpl_asset_model WHERE alpl_asset_model_id = '
                            # if (value == None):
                            #     value = ""
                            # else:
                            #     assetModelIdQuery = assetModelIdQuery + '\'' + item.get(json_column, None) + '\''
                            #
                            #     cursor.execute(assetModelIdQuery)
                            #     assetModelId = cursor.fetchone()
                            #     if (assetModelId == None):
                            #         value = ""
                            #     else:
                            #         value = assetModelId[0]

                        if (table_column == 'tonnage'):
                            value = dictassetTonnage.get(int(assetModelId))

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


                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + validate_string(value, table_column)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'asset_unit'):
                            value = getUnitId(value)


                        if (table_column == 'temp_check'):
                            value = 1

                        if (table_column == 'asset_model_id'):
                            assetModelId = value
                            value = dictassetmodel.get(int(value))
                            # assetModelIdQuery = 'SELECT asset_model_id FROM alpl_uat.alpl_asset_model WHERE alpl_asset_model_id = '
                            # if (value == None):
                            #     value = ""
                            # else:
                            #     assetModelIdQuery = assetModelIdQuery + '\'' + item.get(json_column, None) + '\''
                            #     cursor.execute(assetModelIdQuery)
                            #     assetModelId = cursor.fetchone()
                            #     if (assetModelId == None):
                            #         value = ""
                            #     else:
                            #         value = assetModelId[0]

                        if (table_column == 'tonnage'):
                            value = dictassetTonnage.get(int(assetModelId))

                        if (value == None):
                                value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + ',' + validate_string(value, table_column)


                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where asset_reg_number=' + valueID
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
                cti = cti+1
        except Exception as e:
            print(e)
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
                errorQuery = errorQuery + validate_string('asset_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
            p = p+1
    con.close()
    print("inserted rows:",cti)
    print("updated rows:",ctu)
    print("errors :", errors)
    print("connection asset_master closed successfully and no of rows inserted:",p)

def load_asset_master_mining(json,source):
    print("Data ingestion starting for asset_master")
    columMap = {
        'asset_reg_number': 'RegNo',
        'refid': 'ID',
        'asset_model_id': 'VehicleModelID',
        'asset_reg_date': 'RegDate',
        'asset_valid_till': 'RegDateValidTill',
        'asset_smt_cardid': 'SmartCardId',
        'asset_status': 'StatusID',
        'asset_created_on': 'CreatedDateTIme',
        'asset_modf_date': 'ModifiedDateTime',
        'asset_unit': 'LocationID',
        'asset_manufacturer': 'VehicleMake'
        # 'asset_capacity': 'CAPACITY',
        # 'asset_type': 'TYPE',
        # 'asset_modf_on': 'MODF_ON',
        # 'no_of_tyre': 'NOF_TYRE',
        # 'status_a_i': 'STATUS_A_I',
        # 'no_of_spare_tyre': 'NO_SPA_TYRE',
        # 'ad_blue_cap': 'AD_BLUE_CAP',
        # 'map_tyre': 'MAP_TYRE',
        # 'asset_dec_date': 'TRUCK_DEC_DATE',
        # 'asset_name': 'TRUCK_NAME',
        # 'usage_ind': 'USAGE_IND',
        # 'COMMERCIAL_NONCommercial': 'USAGE_IND_TEXT',
        # 'temp_check': ''
    }

    cursor = con.cursor()
    src = 'mining'
    cursor.execute('SELECT asset_reg_number from alpl_uat.alpl_asset_master')
    result = cursor.fetchall()
    regList = [i[0] for i in result]

    ctu = 0
    cti= 0
    p = 0
    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_asset_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_asset_master SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'asset_reg_number' and (
                        item.get(json_column, None) in regList)):
                    if flag == 0:
                        val = item.get(json_column, None)
                        if (type(val) in (int, float)):
                            val = str(val)
                        if (val == None):
                            val = ""
                        valueID = validate_string_mining(val, table_column)
                        flag = 1
                    if (i == 0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)

                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)

                        updateQuery = updateQuery + '=' + validate_string_mining(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'asset_unit'):
                            if(value == '1'):
                                value=8
                            elif(value == '2'):
                                value=7

                        if(table_column == 'asset_status'):
                            if(value == 1):
                                value = 'ACTIVE'
                            elif(value == 2):
                                value ='SOLD/INACTIVE'


                        if (table_column == 'asset_model_id'):
                            assetModelIdQuery = 'SELECT asset_model_id FROM alpl_uat.alpl_asset_model WHERE alpl_asset_model_id = '
                            if (value == None):
                                value = ""
                            else:
                                assetModelIdQuery = assetModelIdQuery + '\'' + item.get(json_column, None) + '\''

                                cursor.execute(assetModelIdQuery)
                                assetModelId = cursor.fetchone()
                                if (assetModelId == None):
                                    value = ""
                                else:
                                    value = assetModelId[0]

                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        updateQuery = updateQuery + '=' + validate_string_mining(value, table_column)

                    i = 1
                else:
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)


                        if (type(value) in (int, float)):
                            value = str(value)


                        dataset = dataset + validate_string_mining(value, table_column)


                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'asset_unit'):
                            if (value == '1'):
                                value = 8
                            elif (value == '2'):
                                value = 7

                        if (table_column == 'asset_status'):
                            if (value == 1):
                                value = 'ACTIVE'
                            elif (value == 2):
                                value = 'SOLD/INACTIVE'

                        if (table_column == 'asset_model_id'):
                            assetModelIdQuery = 'SELECT asset_model_id FROM alpl_uat.alpl_asset_model WHERE alpl_asset_model_id = '
                            if (value == None):
                                value = ""
                            else:
                                assetModelIdQuery = assetModelIdQuery + '\'' + item.get(json_column, None) + '\''
                                cursor.execute(assetModelIdQuery)
                                assetModelId = cursor.fetchone()
                                if (assetModelId == None):
                                    value = ""
                                else:
                                    value = assetModelId[0]

                        if (value == None):
                                value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + ',' + validate_string_mining(value, table_column)


                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where asset_reg_number=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                print(q)
                cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                print(q)
                cursor.execute(totalQuery)
                cti = cti+1
        except Exception as e:
            print(e)
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
                errorQuery = errorQuery + validate_string('asset_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
            p = p+1
    con.close()
    print("inserted rows:",cti)
    print("updated rows:",ctu)
    print("errors :", errors)
    print("connection asset_master closed successfully and no of rows inserted:",p)

def load_leg_master(json,source):
    print("Data ingestion starting for alpl_leg_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    q = None
    org = None
    src = 'logistics'
    errors = 0
    if ("MINING" in json_obj[source]):
        src = 'mining'
    columMap = {'alpl_leg_id': 'id',
    'from_location': 'From_LocationID',
    'description': 'Description',
    'to_location': 'To_LocationID',
    'created_user_id' : 'CreatedUserID',
    'created_date_time': 'CreatedDateTime',
    'distance': 'distance',
    'target_time_in_mins': 'TargetTimeinMins'}
    type=json_obj[source]
    if re.match("MINING",type):
        org=2
    else:
        org=1
    listQuery="SELECT alpl_leg_id from alpl_uat.alpl_leg_master where org_id="+str(org)
    cursor.execute(listQuery)
    result=cursor.fetchall()
    legIdList=[i[0] for i in result]
    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_leg_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_leg_master SET '
        i = 0
        flag = 0
        valueID = '0'
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'alpl_leg_id' and (int(item.get(json_column,None)) in legIdList)):
                    if flag == 0:
                        value = item.get(json_column, None)
                        valueID = validate_string_vendor_location(value, table_column,org)
                        flag = 1
                    if(i==0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)
                        updateQuery = updateQuery + '=' + validate_string_vendor_location(value, table_column,org)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
                        updateQuery = updateQuery + '=' + validate_string_vendor_location(value, table_column,org)
                    i = 1
                else:
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)
                        dataset = dataset + validate_string_vendor_location(value, table_column,org)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        dataset = dataset + ',' + validate_string_vendor_location(value, table_column,org)
                    i = 1
            query=query+','+"org_id"
            dataset=dataset+','+str(org)
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where alpl_leg_id=' + valueID
            updateQuery= updateQuery +' and org_id='+str(org)
            totalQuery = query + dataset
            if(flag == 1):
                cursor.execute(updateQuery)
                # print(updateQuery)
            else:
                cursor.execute(totalQuery)
                # print(totalQuery)
        except Exception as e:
            print(e,q)
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
                errorQuery = errorQuery + validate_string('alpl_leg_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            print("data inserted successfully")
            p = p + 1
            con.commit()
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_leg_master:", p)
    print("errors :", errors)
    print("connection closed successfully")

def load_leg_time_master(json,source):
    print("Data ingestion starting for alpl_leg_time_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    p = 0
    errors = 0
    column_map = {'leg_id': 'LegID',
    'vehicle_model': 'VehicleModelID',
    'speed': 'Speed',
    'created_user_id' : 'CreatedUserID',
    'created_date_time': 'CreatedDateTime',
    'target_time_in_min': 'TargetTimeinMins'}

    for i, item in enumerate(json):

        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_leg_time_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_leg_time_master SET '
        i = 0
        flag = 0
        valueID = '0'
        id = 0
        model = 0
        q = None
        try:

            assetIdQuery = 'SELECT asset_model_id FROM alpl_uat.alpl_asset_model WHERE alpl_asset_model_id = '
            assetIdQuery = assetIdQuery + item.get('VehicleModelID', None)
            cursor.execute(assetIdQuery)
            assetId = cursor.fetchone()
            if(assetId == None):
                model = 'NULL'
            else:
                model = assetId[0]

            legIdQury = 'SELECT leg_master_id FROM alpl_uat.alpl_leg_master WHERE alpl_leg_id = '
            legIdQury = legIdQury + item.get('LegID', None)
            cursor.execute(legIdQury)
            legId = cursor.fetchone()
            if(legId == None):
                id = 'NULL'
            else:
                id=legId[0]


            searchQuery = 'SELECT count(*) from alpl_uat.alpl_leg_time_master where (leg_id = '
            searchQuery = searchQuery +  str(id) + ' or leg_id is NULL)'
            searchQuery = searchQuery + ' and (vehicle_model = '
            searchQuery = searchQuery +  str(model) + ' or vehicle_model is NULL)'
            print(searchQuery)
            cursor.execute(searchQuery)
            count = cursor.fetchone()
            if(count == None):
                check = 0
            else:
                check = count[0]

            for table_column, json_column in column_map.items():

                if (flag == 1) or (check > 0):
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
                        if (table_column == 'leg_id'):
                            value = id

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'vehicle_model'):
                            value = model

                        if (type(value) in (int, float)):
                            value = str(value)

                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    i = 1
                elif (check == 0):
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'leg_id'):
                            value = id
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + validate_string(value, table_column)
                        print(dataset)
                        print(query)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'vehicle_model'):
                            value = model

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                        print(dataset)
                        print(query)
                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where leg_id=' + str(id) + ' and vehicle_model = ' + str(model)
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                print(q)
                cursor.execute(updateQuery)
            else:
                q = totalQuery
                print(q)
                cursor.execute(totalQuery)
        except Exception as e:
            print(e, i)
            print(q)
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
                errorQuery = errorQuery + validate_string('alpl_leg_time_master', 'tab') + ',' + str(
                    validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            print("data inserted successfully")
            con.commit()
            p = p + 1

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_leg_time_master:", p)
    print("connection closed successfully")


def asset_model(json,source):
    print("Data ingestion starting for alpl_asset_model")
    cursor = con.cursor()

    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'

    column_map = {'alpl_asset_model_id': 'VEH_MODEL_ID',
                  'asset_model_name': 'VEH_NAME',
                  'asset_manufacturer':'MANUFACTURER',
                  'max_load_capacity':'MAX_LOAD_CAPACITY',
                  'max_return_load_capacity':'MAX_RETURN_LOAD_CAPACITY'
                  }

    cursor.execute('SELECT alpl_asset_model_id from alpl_uat.alpl_asset_model')
    result = cursor.fetchall()
    assetIdList = [i[0] for i in result]
    p= 0
    ctu =0
    cti = 0
    errors = 0
    cursor = con.cursor()
    for i, item in enumerate(json):
        dataset = 'values ('
        insertQuery = 'INSERT INTO alpl_uat.alpl_asset_model ('
        updateQuery = 'UPDATE alpl_uat.alpl_asset_model SET '
        i = 0
        flag = 0
        valueID = '0'
        value = None
        q = None
        try:
            for table_column, json_column in column_map.items():

                if (flag == 1) or (table_column == 'alpl_asset_model_id' and (
                        int(validate_string(item.get(json_column, None), table_column)) in assetIdList)):
                    if flag == 0:
                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        valueID = int(validate_string(item.get(json_column, None),table_column))
                    flag = 1
                    if i == 0:
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)
                        if(value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1
                else:
                    if i == 0:
                        insertQuery = insertQuery + table_column
                        value = item.get(json_column, None)
                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        insertQuery = insertQuery + ',' + table_column
                        value = item.get(json_column, None)
                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1
            dataset = dataset + ')'
            insertQuery = insertQuery + ')'
            updateQuery = updateQuery + ' where alpl_asset_model_id=' + str(valueID)
            totalQuery = insertQuery + dataset
            if flag:
                q = updateQuery
                #cursor.execute(updateQuery)
                #print(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                print(totalQuery)
                cti= cti+1

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
                errorQuery = errorQuery + validate_string('alpl_asset_model', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
            p= p + 1
    con.close()
    print("connection closed successfully")
    print("update :",ctu)
    print("insert :",cti)
    print("no of rows processed are :",p)
    print("errors :", errors)

def load_location_master(json,source):
    print("Data ingestion starting for alpl_location_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    errors = 0
    org = 1
    if ("MINING" in json_obj[source]):
        src = 'mining'
        org = 2
    p = 0
    columMap = {'alpl_reference_id': 'ALPLRefId', 'location_name': 'Name', 'location_code': 'LocationCode',
                'location_type_id': 'LocationTypeId', 'rcl_cust_id': 'RCLCustId', 'address_line1': 'AddressLine1',
                'address_line2': 'AddressLine2', 'city_id': 'CityID', 'district_id': 'DistrictID',
                'state_id': 'StateID', 'country_id': 'CountryID', 'pincode': 'Pincode', 'contact_no': 'ContactNo',
                'gst_no': 'GSTNo', 'created_user_id': 'CreatedUserId', 'created_date_time': 'CreatedDateTime'}
    type = json_obj[source]

    listQuery = "SELECT alpl_reference_id from alpl_uat.alpl_location_master where org_id=" + str(org)
    cursor.execute(listQuery)
    result = cursor.fetchall()
    locationIdList = [i[0] for i in result]
    for i, item in enumerate(json):
        dataset='values ('
        insertQuery='INSERT INTO alpl_uat.alpl_location_master ('
        updateQuery='UPDATE alpl_uat.alpl_location_master SET '
        j=0
        flag=0
        value='0'
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (
                        table_column == 'alpl_reference_id' and (int(item.get(json_column, None)) in locationIdList)):
                    if flag == 0:
                        value = validate_string_vendor_location(item.get(json_column, None), table_column, org)
                    flag = 1
                    if j == 0:
                        updateQuery = updateQuery + table_column
                        updateQuery = updateQuery + '=' + validate_string_vendor_location(item.get(json_column, None),
                                                                                          table_column, org)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        updateQuery = updateQuery + '=' + validate_string_vendor_location(item.get(json_column, None),
                                                                                          table_column, org)

                    j = 1
                else:
                    if j == 0:
                        insertQuery = insertQuery + table_column
                        dataset = dataset + validate_string_vendor_location(item.get(json_column, None), table_column,
                                                                            org)
                    else:
                        insertQuery = insertQuery + ',' + table_column
                        dataset = dataset + ',' + validate_string_vendor_location(item.get(json_column, None),
                                                                                  table_column, org)

                    j = 1
            insertQuery = insertQuery + ',' + "org_id"
            dataset = dataset + ',' + str(org)
            dataset = dataset + ')'
            insertQuery = insertQuery + ')'
            updateQuery = updateQuery + ' where alpl_reference_id=' + value
            updateQuery = updateQuery + ' and org_id=' + str(org)
            totalQuery = insertQuery + dataset
            if flag:
                cursor.execute(updateQuery)
                # print(updateQuery)
            else:
                cursor.execute(totalQuery)
                # print(totalQuery)

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
                errorQuery = errorQuery + validate_string('alpl_location_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
        print("Data ingestion completed. No of rows processed for alpl_location_master:", p)
        print("errors :", errors)
        print("connection closed successfully")

def load_location_type_master(json,source):
    print("Data ingestion starting for alpl_location_type_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    columMap={'alpl_reference_id':'ALPLRefId','location_name':'Name','location_type':'LocationType'}
    type=json_obj[source]
    if re.match("MINING",type):
        org=2
    else:
        org=1
    print(org)
    listQuery="SELECT alpl_reference_id from alpl_uat.alpl_location_type_master where org_id="+str(org)
    cursor.execute(listQuery)
    result=cursor.fetchall()
    locationTypeIdList=[i[0] for i in result]
    for i, item in enumerate(json):
        dataset=' values ('
        insertQuery='INSERT INTO alpl_uat.alpl_location_type_master ('
        updateQuery='UPDATE alpl_uat.alpl_location_type_master SET '
        i=0
        flag=0
        value='0'
        try:
            for table_column,json_column in columMap.items():
                if (flag==1) or (table_column=='alpl_reference_id' and (int(item.get(json_column,None)) in locationTypeIdList)):
                    if flag==0:
                        value=validate_string_vendor_location(item.get(json_column,None),table_column,org)
                    flag=1
                    if i==0:
                        updateQuery=updateQuery+table_column
                        updateQuery=updateQuery+'='+validate_string_vendor_location(item.get(json_column,None),table_column,org)
                    else:
                        updateQuery=updateQuery+','+table_column
                        updateQuery=updateQuery+'='+validate_string_vendor_location(item.get(json_column,None),table_column,org)

                    i=1
                else:
                    if i==0:
                        insertQuery=insertQuery+table_column
                        dataset=dataset+validate_string_vendor_location(item.get(json_column,None),table_column,org)
                    else:
                        insertQuery=insertQuery+','+table_column
                        dataset=dataset+','+validate_string_vendor_location(item.get(json_column,None),table_column,org)

                    i=1
            insertQuery=insertQuery+','+"org_id"
            dataset=dataset+','+str(org)
            dataset=dataset+')'
            insertQuery=insertQuery+')'
            updateQuery=updateQuery+' where alpl_reference_id='+value
            updateQuery=updateQuery+' and org_id='+str(org)
            totalQuery=insertQuery+dataset
            if flag:
                cursor.execute(updateQuery)
                # print(updateQuery)
            else:
                cursor.execute(totalQuery)
                # print(totalQuery)

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
                errorQuery = errorQuery + validate_string('alpl_location_type_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_location_type_master:", p)
    print("errors :", errors)
    print("connection closed successfully")

def load_trip_route_leg_trans(json,source):
    print("Data ingestion starting for alpl_trip_route_legs_transaction")
    #json_obj = getJsonFile(json,source)
    tripRouteLegLists = []
    cursor = con.cursor()
    src = "logistics"
    orgId = 1
    if ("MINING" in json_obj[source]):
        src = 'mining'
        orgId = 2

    cursor.execute('select CONCAT(alpl_trip_id,route_path_id,leg_id) from  alpl_uat.alpl_trip_route_legs_transaction ')
    res=cursor.fetchall()
    tripRouteLegLists = [i[0] for i in res]
    ctu = 0
    cti = 0
    p = 0
    sts = 0
    legQ=""
    mining=False
    errors = 0
    columMap={'alpl_trip_id':'TripID','route_path_id':'RoutePathID','leg_id':'LegID','sequence_num':'SeqNo',
              'distance':'Distance','target_duration_min':'TargetDurationinMin','arrival_time':'ArrivalDateTime',
              'departure_time':'DepartureDateTime'}
    statusDict={'UNDEPT':'AT UNIT','DEPTDEPT':'AT UNIT','DEPTFR':'AT FACTORY','FRACT':'ACTIVITY','ACTFR':'FACTORY OUT','FRCO':'TOWARDS CONTROL POINT','CODEPT':'TOWARDS UNIT'}
    towardsDict={'FR':'TOWARDS FACTORY','DEPT':'TOWARDS UNIT','GDN':'TOWARDS GODOWN','CO':'TOWARDS CONTROL POINT','DLR':'TOWARDS DEALER','CONDES':'TOWARDS CONSIGNEE'}
    updatedStatusDict={'UNDEPT':'At Unit','DEPTDEPT':'At Unit','DEPTFR':'At Factory'}
    if("MINING" in json_obj[source] ):
        legQ='select leg_master_id from alpl_uat.alpl_leg_master where org_id=2 and alpl_leg_id='
        mining=True
    else:
        legQ='select leg_master_id from alpl_uat.alpl_leg_master where org_id=1 and alpl_leg_id='
    for i, item in enumerate(json):

        dataset='values ('
        query='INSERT INTO alpl_uat.alpl_trip_route_legs_transaction ('
        updateQuery = 'UPDATE alpl_uat.alpl_trip_route_legs_transaction SET '
        i = 0
        flag = 0
        valueID = '0'
        tripID = 0
        routhPathId = 0
        legId = 0
        leg=0
        checkValue = None
        try:
            for table_column,json_column in columMap.items():
                if(table_column == 'alpl_trip_id'):
                    tripID = item.get(json_column, None)
                if(table_column == 'route_path_id'):
                    routhPathId = item.get(json_column, None)
                if(table_column == 'leg_id'):
                    legId = item.get(json_column, None)
                    if(legId is not None):
                        cursor.execute(legQ+legId)
                        res=cursor.fetchall()
                        if res== []:
                            raise Exception("Leg Id not present in db",legId)
                        leg=res[0][0]
                        checkValue = str(tripID)+str(routhPathId)+str(leg)



                if(flag == 1) or (table_column == 'sequence_num' and str(checkValue) in tripRouteLegLists):
                    if(i==0):

                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                        flag =1
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""

                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1
                elif(table_column != 'alpl_trip_id' and table_column != 'route_path_id' and table_column != 'leg_id'):
                    if i == 0:

                        query = query + table_column
                        value = item.get(json_column, None)


                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if (value == None):
                                value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1
                # if(table_column == 'arrival_time'):
                #     v = item.get(json_column, None)
                #     v = datetime.strptime(v, '%Y-%m-%d %I:%M:%S %p')
                #     arrivalTimeQuery = "select max(arrival_time) from alpl_uat.alpl_trip_route_legs_transaction where alpl_trip_id = "+ str(tripID) + ' and route_path_id=' + str(routhPathId)
                #     cursor.execute(arrivalTimeQuery)
                #     atime = cursor.fetchone()
                #     print(arrivalTimeQuery)
                #     atimevalue = None
                #     if (atime == None):
                #         atimevalue = v
                #         print('HII')
                #     else:
                #         atimevalue = atime[0]
                #         print('HI')
                #
                #     #atimevalue = datetime.strptime(atimevalue, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
                #     print("curr")
                #     print(v)
                #     print("max time")
                #     print(atimevalue)
                #     if(v >= atimevalue):
                #         statusquery = "update alpl_asset_telemetry set status = (select location_type from alpl_location_type_master where alpl_location_type_id = (select location_type_id from alpl_location_master "
                #         statusquery = statusquery + " where alpl_location_master_id =(select to_location from alpl_leg_master where alpl_leg_id =" + legId + ")))where trip_id =" + tripID + " and "
                #         statusquery = statusquery + " aat_id = (select aat_id from alpl_asset_telemetry where trip_id = (select trip_id from alpl_trip_data where alpl_trip_id = " + tripID + " ) order by created_time desc limit 1)"
                #         print(statusquery)
                #         sts = sts+1
                        #cursor.execute(statusquery)

            dataset=dataset+','+str(tripID)+','+str(routhPathId)+','+str(leg)+')'
            query=query+',alpl_trip_id,route_path_id,leg_id)'
            totalQuery=query+dataset
            updateQuery = updateQuery + ' where alpl_trip_id=' + str(tripID) + ' and route_path_id=' + str(routhPathId) + ' and leg_id=' + str(leg)
            if (flag == 1):
                q = updateQuery
                cursor.execute(updateQuery)
                ctu = ctu + 1
                #print(q)
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                cti = cti + 1
                #print(q)
                print(cti)
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
                errorQuery = errorQuery + validate_string('alpl_trip_route_leg_trans', 'tab') + ',' + str(
                    validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)

            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            if(validate_string_vendor_location(item.get('ArrivalDateTime', None), 'arrival_time',orgId)!='NULL'):
                statusQ='select concat(lt1.location_type,lt2.location_type) as key,l2.location_name as toLoc,lt1.location_name as fromLoc,lt2.location_type from alpl_leg_master lm,alpl_location_master l1,alpl_location_master l2,alpl_location_type_master lt1,alpl_location_type_master lt2 '+'where lm.from_location=l1.alpl_location_master_id and lm.to_location=l2.alpl_location_master_id and l1.location_type_id=lt1.alpl_location_type_id and '+'l2.location_type_id=lt2.alpl_location_type_id and lm.leg_master_id='+str(leg)
                seq=int(item.get('SeqNo',None))+1
                towardsQ='select lm.leg_master_id,lt.location_name,trl.distance,trl.route_path_id,l.location_name from alpl_trip_route_legs_transaction trl,alpl_leg_master lm,alpl_location_master l,alpl_location_type_master lt where trl.leg_id=lm.leg_master_id and lm.to_location=l.alpl_location_master_id and l.location_type_id=lt.alpl_location_type_id and trl.sequence_num='+str(seq)+' and trl.alpl_trip_id='+(item.get('TripID',None))
                cursor.execute(statusQ)
                result1=cursor.fetchall()
                print(result1)
                cursor.execute(towardsQ)
                result2=cursor.fetchall()
                print(result2)
                leg2=result2[0][0]
                type2=result2[0][1]
                dist=result2[0][2]
                route2=result2[0][3]
                toLocName2=result2[0][4]
                key1=result1[0][0]
                toLoc1=result1[0][1]
                fromLoc1=result1[0][2]
                type1=result1[0][3]
                if(mining and key1=="LODPTACT"):
                    tripUpdateQuery="update alpl_trip_data set is_completed=1,trip_close_time="+validate_string_vendor_location(item.get('ArrivalDateTime', None), 'arrival_time',orgId)+" where alpl_trip_id="+(item.get('TripID',None))+" and is_completed=0"
                    tripInsertionQuery="INSERT INTO alpl_dev_redesign.alpl_trip_data(alpl_trip_id, asset_reg_number, driver_id, vigilance_officer_id, master_route_id, advance_amount, ind_quality, quantity, recovery, exemption_amount, budget, actual_expense, target_kmpl, actual_kmpl, created_time, last_updated_time, target_liters, alpl_trip_datacol, route_from, route_to, ad_blue, actual_kms, created_by, last_updated_by, actual_unloading_locations, assettype, actual_kms_coverage,"+ "no_of_unloading_location, unit_id, int_order_id, int_indent_issued, int_indent_date,"+ "int_price, ext_order_id, ext_indent_issued, ext_indent_date, ext_price, internal_order_id) (select alpl_trip_id, asset_reg_number, driver_id, vigilance_officer_id, master_route_id, advance_amount, ind_quality, quantity, recovery, exemption_amount, budget, actual_expense, target_kmpl, actual_kmpl, created_time, last_updated_time, target_liters, alpl_trip_datacol, route_from,route_to,"+ "ad_blue, actual_kms, created_by, last_updated_by, actual_unloading_locations, assettype, actual_kms_coverage, no_of_unloading_location, unit_id, int_order_id, int_indent_issued,"+ "int_indent_date,int_price, ext_order_id, ext_indent_issued, ext_indent_date, ext_price, internal_order_id from alpl_trip_data where alpl_trip_id="+(item.get('TripID',None))+" order by trip_creation_time desc limit 1)"
                    cursor.execute(tripUpdateQuery)
                    cursor.execute(tripInsertionQuery)
                    cursor.execute("select driver_id from alpl_asset_telemetry where trip_id=22846 order by created_time desc limit 1")
                    result=cursor.fetchall()
                    currentDriverId=result[0][0]
                    if(currentDriverId==None):
                        currentDriverId='NULL'
                    cursor.execute("update alpl_trip_data set is_completed=0,"+"driver_id="+str(currentDriverId)+",trip_creation_time="+validate_string_vendor_location(item.get('ArrivalDateTime', None), 'arrival_time',orgId)+" where trip_creation_time is null and alpl_trip_id="+(item.get('TripID',None)))
                if(dist==0):
                    if(type1=='ACT'):
                        st=toLoc1
                    else:
                        if key1 in updatedStatusDict.keys():
                            st=updatedStatusDict[key1]
                        else:
                            st=key1
                    leg=item.get('LegID',None)
                    route=item.get('RoutePathID',None)
                else:
                    if(type2=="DEPARTMENT"):
                        st='Towards Unit'
                    elif(type2=="Location"):
                        st='Towards '+toLocName2
                    elif(mining and type2=="Loading Point"):
                        st='Towards Excavator'
                    else:
                        st='Towards '+type2
                    leg=leg2
                    route=route2
                upStatus='update alpl_asset_telemetry set status='+"'"+st+"'"+', leg_id='+str(leg)+', route_path_id='+str(route)+' where trip_id=(select trip_id from alpl_trip_data where alpl_trip_id='+(item.get('TripID',None))+') and created_time in (select max(created_time) from alpl_asset_telemetry where trip_id=(select trip_id from alpl_trip_data where alpl_trip_id='+(item.get('TripID',None))+'))'
                print(upStatus)
                cursor.execute(upStatus)
            con.commit()
            p=p+1
    updateTrip = 'update alpl_trip_route_legs_transaction l set trip_id = (select t.trip_id from alpl_trip_data t  where t.alpl_trip_id=l.alpl_trip_id ) where trip_id is null'
    #cursor.execute(updateTrip)
    con.commit()
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_trip_route_legs_transaction:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)
    print("status:",sts)
    print("error:",errors)



def load_parts_master(json,source):
    print("Data ingestion starting for alpl_parts_master")
    # json_obj = getJsonFile(json,source)
    partsLists = []
    cursor = con.cursor()

    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'

    cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_uat.alpl_unit_master')
    result = cursor.fetchall()
    unitCodeList = [i[0] for i in result]
    unitIdList = [i[1] for i in result]

    dictUnitc = {}
    length = len(unitCodeList)
    for x in range(length):
        dictUnitc[unitCodeList[x]] = unitIdList[x]

    cursor.execute('SELECT unit_id,id FROM alpl_uat.alpl_unit_dept_mapping')
    result = cursor.fetchall()
    unitList = [i[0] for i in result]
    udIdList = [i[1] for i in result]

    dictUDId = {}
    length = len(unitList)
    for x in range(length):
        dictUDId[unitList[x]] = udIdList[x]

    cursor.execute('SELECT location,parts_location_master_id FROM alpl_uat.alpl_parts_location_master')
    result = cursor.fetchall()
    locationList = [i[0] for i in result]
    locationIdList = [i[1] for i in result]

    dictLoc = {}
    length = len(locationList)
    for x in range(length):
        dictLoc[locationList[x]] = locationIdList[x]

    cursor.execute('select concat(batch,alpl_part_id,store_id,unit_dept_id) from  alpl_uat.alpl_parts_master ')
    res = cursor.fetchall()
    partsLists = [i[0] for i in res]
    ctu = 0
    cti = 0
    p = 0
    errors = 0

    columMap = {'alpl_part_id': 'PART_ID',
                'batch': 'BATCH',
                'store_id': 'STORE_ID',
                'unit_dept_id': 'UNIT_NUM',
                'part_name': 'PART_NAME',
                'con_barcode': 'CON_BARCODE',
                'part_type': 'PART_TYPE',
                'part_num': 'PART_NUM',
                'part_category': 'PART_CATG',
                'part_category_desc':'PART_CATG_DESC',
                'part_type_desc':'PART_TYPE_DESC',
                'part_sub_category': 'PART_SUB_CATG_DESC',
                'manufacturer_name': 'MANFAC_NAME',
                'supplier_id': 'SUPPLIER_ID',
                'purchase_date': 'PURC_DATE',
                'expiry_date': 'EXP_DATE',
                'store_keeper_id': 'STORE_KEEPER_ID',
                'msl': 'MSL',
                'abc_class': 'ABC_CLASS',
                'cost': 'COST',
                'stock_value': 'STOCK_VAL',
                'closing_stock': 'CLOS_STOCK',
                'aging_part': 'AGING_PART',
                'upt_req': 'UPT_REQ',
                'decomiss_date': 'DECOMISS_DATE',
                'moving_price': 'MOVING_PRICE',
                'unit_id': 'UNIT_NUM',
                'grn_date': 'GRN_DATE',
                'part_sub_cat_group':'AGG_GRP',
                'part_sub_cat_code':'PART_SUB_CATG',
                'purchasing_group_code':'PUR_GROUP',
                'purchasing_group_desc':'PUR_GRP_DESC'}

    for i, item in enumerate(json):
        # print('_____________________________________________________')
        dataset = ' values ('
        insertQuery = 'INSERT INTO alpl_uat.alpl_parts_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_parts_master SET '
        i = 0
        flag = 0
        valueID = '0'
        batch = None
        partId = None
        storeId = None
        unitId = None
        updatestatement = None
        fg = 0
        q = None
        if (fg == 0):

            batch = item.get('BATCH', None)
            if (batch == ''):
                batch = ''
            else:
                batch = str(batch)
            partId = item.get('PART_ID', None)
            if (partId == ''):
                partId = ''
            else:
                partId = str(partId)

            storeId = item.get('STORE_ID', None)
            if (storeId == ''):
                storeId = ''
            else:
                storeId = str(dictLoc.get(int(storeId)))

            unitId = item.get('UNIT_NUM', None)
            if (unitId == '' or unitId == 'ALPL'):
                unitId = ''
            else:
                unitId = getUnitId(int(unitId))
                unitId = str(dictUDId.get(int(unitId)))

            updatestatement = str(batch) + str(partId) + str(storeId) + str(unitId)
            fg = 1
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'alpl_part_id' and updatestatement in partsLists):
                    if flag == 0:
                        value = item.get(json_column, None)
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        valueID = validate_string(value, table_column)
                    flag = 1
                    if i == 0:
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'unit_dept_id'):
                            value = unitId
                            if (unitId == None):
                                unitId = ""

                            if (type(unitId) in (int, float)):
                                unitId = str(unitId)
                            unitId = validate_string(unitId, table_column)

                        if (table_column == 'store_id'):
                            value = storeId
                            if (storeId == None):
                                storeId = ""

                            if (type(storeId) in (int, float)):
                                storeId = str(storeId)
                            storeId = validate_string(storeId, table_column)

                        if(table_column == 'batch' or batch == ''):
                            if (batch == ''):
                                batch = ""
                                batch = validate_string(batch, table_column)

                            batch = "'"+str(batch)+"'"



                        if (table_column == 'unit_id' ):
                            if(value != 'ALPL'):
                                value = getUnitId(int(value))
                            else:
                                value = ''


                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        if (table_column != 'batch'):
                            updateQuery = updateQuery + '=' + validate_string(value, table_column)
                        else:
                            if (value == ""):
                                updateQuery = updateQuery + '=' + validate_string(value, table_column)
                            else:
                                updateQuery = updateQuery + '=' + "'" + value + "'"

                    i = 1
                else:
                    if i == 0:

                        insertQuery = insertQuery + table_column
                        value = item.get(json_column, None)

                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        insertQuery = insertQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'unit_dept_id'):
                            value = unitId
                            if (unitId == None):
                                unitId = ""

                            if (type(unitId) in (int, float)):
                                unitId = str(unitId)
                            unitId = validate_string(unitId, table_column)

                        if (table_column == 'store_id'):
                            value = storeId
                            if (storeId == None):
                                storeId = ""

                            if (type(storeId) in (int, float)):
                                storeId = str(storeId)
                            storeId = validate_string(storeId, table_column)

                        if (table_column == 'batch' or batch == ''):
                            if (batch == ''):
                                batch = ""
                                batch = validate_string(batch, table_column)

                            batch = "'" + str(batch) + "'"

                        if (table_column == 'unit_id' ):
                            if(value != 'ALPL'):
                                value = getUnitId(int(value))
                            else:
                                value = ''


                        # if(table_column == 'batch'):
                        #     v = batch + 'E'
                        #     v = str(validate_string(v, table_column))
                        #     v = str(v[0:-2]) + "'"
                        #     print(v)

                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (table_column != 'batch'):
                            dataset = dataset + ',' + validate_string(value, table_column)
                        else:
                            if (value == ""):
                                dataset = dataset + ',' + validate_string(value, table_column)
                            else:
                                dataset = dataset + ',' + "'" + value + "'"

                    i = 1
            dataset = dataset + ')'
            insertQuery = insertQuery + ')'
            updateQuery = updateQuery + ' where alpl_part_id=' + valueID + ' and batch = ' + str(batch)  + ' and store_id =' + str((storeId))
            updateQuery = updateQuery + ' and unit_dept_id = ' + str(unitId)
            totalQuery = insertQuery + dataset
            if flag:
                q = updateQuery
                cursor.execute(updateQuery)
                ctu = ctu + 1
                #print(updateQuery)
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                cti = cti + 1
                #print(totalQuery)
        except Exception as e:
            print(e)
            print(p)
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
                errorQuery = errorQuery + validate_string('alpl_parts_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p+ 1
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_parts_master:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)
    print("Errors :",errors)


def load_parts_trans(json,source):
    print("Data ingestion starting for alpl_parts_trans")
    #json_obj = getJsonFile(json,source)
    partsLists = []
    cursor = con.cursor()
    cursor.execute('select alpl_part_trans_id from  alpl_uat.alpl_parts_trans ')
    res=cursor.fetchall()
    partsTransLists = [i[0] for i in res]
    ctu = 0
    cti = 0
    p = 0

    columMap={'alpl_part_trans_id':'ALPLREFID',
              'batch':'BATCH',
              'part_id':'PART_ID',
              'maintenance_indent_id':'MAINT_INDENTID',
              'unit_id':'UNIT_NUM',
              'store_code':'STORE_ID',
              'fitment_date':'FITMENT_DATE',
              'stock_val':'STOCK_VAL',
              'close_stock':'CLOS_STOCK',
              'aging_part':'AGING_PART',
              'upt_req':'UPT_REQ',
              'decomiss_date':'DECOMISS_DATE',
              'cost':'COST',
              'moving_price':'STORE_ID',
              'grn_date':'GRN_DATE'}



    for i, item in enumerate(json):
        #print('_____________________________________________________')
        dataset = ' values ('
        insertQuery = 'INSERT INTO alpl_uat.alpl_parts_trans ('
        updateQuery = 'UPDATE alpl_uat.alpl_parts_trans SET '
        i = 0
        flag = 0
        valueID = '0'
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'alpl_part_trans_id' and (item.get(json_column, None) in partsTransLists)):
                    if flag == 0:
                        valueID = validate_string(item.get(json_column, None), table_column)
                    flag = 1
                    if i == 0:
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)
                        if (value == None):
                                value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
                        if(table_column == 'unit_num'):
                            value =getUnitId(value)
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1
                else:
                    if i == 0:
                        insertQuery = insertQuery + table_column
                        value = item.get(json_column, None)
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        insertQuery = insertQuery + ',' + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'unit_num'):
                            value =getUnitId(value)
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1
            dataset = dataset + ')'
            insertQuery = insertQuery + ')'
            updateQuery = updateQuery + ' where alpl_part_trans_id=' + valueID
            totalQuery = insertQuery + dataset
            if flag:
                cursor.execute(updateQuery)
                ctu = ctu + 1
                #print(updateQuery)
            else:
                cursor.execute(totalQuery)
                cti = cti + 1
               # print(totalQuery)
        except Exception as e:
            print(e)
            #con.rollback()

        else:
            con.commit()
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_parts_trans:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)


def status_update(json,source):
    print("Data ingestion starting for alpl_trip_route_legs_transaction")
    #json_obj = getJsonFile(json,source)
    tripRouteLegLists = []
    cursor = con.cursor()
    # cursor.execute('select CONCAT(alpl_trip_id,route_path_id,leg_id) from  alpl_uat.alpl_trip_route_legs_transaction ')
    # res=cursor.fetchall()
    # tripRouteLegLists = [i[0] for i in res]
    ctu = 0
    cti = 0
    p = 0
    sts = 0

    columMap={'alpl_trip_id':'TripID','route_path_id':'RoutePathID','leg_id':'LegID','sequence_num':'SeqNo',
              'distance':'Distance','target_duration_min':'TargetDurationinMin','arrival_time':'ArrivalDateTime',
              'departure_time':'DepartureDateTime'}




    try:
        qry = "select distinct alpl_trip_id,leg_id from alpl_trip_route_legs_transaction as ls where arrival_time = (Select max(arrival_time) from alpl_trip_route_legs_transaction as ls2 where ls.alpl_trip_id = ls2.alpl_trip_id)"
        # print(qry)
        cursor.execute(qry)
        result = cursor.fetchall()
        tripList = [i[0] for i in result]
        legList = [i[1] for i in result]

        length = len(tripList)
        #print(length)
        for x in range(length):
            statusQuery = "update alpl_asset_telemetry set status = (select location_name from alpl_location_type_master where alpl_location_type_id = "
            statusQuery = statusQuery + "( select location_type_id from alpl_location_master where alpl_location_master_id = ( select to_location from alpl_leg_master where alpl_leg_id = "
            statusQuery = statusQuery + str(legList[x])
            statusQuery = statusQuery + ")))" + "where trip_id = (select trip_id from alpl_trip_data where alpl_trip_id =" + str(
                tripList[x]) + " order by created_time desc limit 1)"
            # print(statusQuery)
            cursor.execute(statusQuery)

    except Exception as e:
        print(e)
    else:
        con.commit()
        p = p + 1

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_trip_route_legs_transaction:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)
    print("status:",sts)


def load_shift_schedule(json,source):
    print("Data ingestion starting for alpl_shift_schedule")
    #json_obj = getJsonFile(json,source)
    partsLists = []
    cursor = con.cursor()
    # cursor.execute('select alpl_part_id from  alpl_uat.alpl_parts_master ')
    # res=cursor.fetchall()
    # partsLists = [i[0] for i in res]
    ctu = 0
    cti = 0
    p = 0
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'

    columMap ={'alpl_emp_id':'PERNR',
              'emp_name':'ZENAME',
              'year':'ZYEAR',
              'month':'ZMONTH',
              'type':'ZBSHIFT',
              'unit_id':'ZWERKS',
              'day_1':'Z01',
              'day_2':'Z02',
               'day_3':'Z03',
               'day_4':'Z04',
               'day_5':'Z05',
               'day_6':'Z06',
               'day_7':'Z07',
               'day_8':'Z08',
               'day_9':'Z09',
               'day_10':'Z10',
               'day_11':'Z11',
               'day_12':'Z12',
               'day_13':'Z13',
               'day_14':'Z14',
               'day_15':'Z15',
               'day_16':'Z16',
               'day_17':'Z17',
               'day_18':'Z18',
               'day_19':'Z19',
               'day_20':'Z20',
               'day_21':'Z21',
               'day_22':'Z22',
               'day_23':'Z23',
               'day_24':'Z24',
               'day_25':'Z25',
               'day_26':'Z26',
               'day_27':'Z27',
               'day_28':'Z28',
               'day_29':'Z29',
               'day_30':'Z30',
               'day_31':'Z31'
    }

    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_uat.alpl_shift_schedule ('
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

                    if (table_column == 'unit_id'):
                        value = getUnitId(value)

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            # print(query)
            # print(dataset)
            cursor.execute(totalQuery)
        except Exception as e:
            print(e, i)
            print(query)
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
                errorQuery = errorQuery + validate_string('alpl_shift_scheduler', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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

    print("Data ingestion completed. No of rows processed for alpl_shift_scheduler:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)
    print("errors :", errors)

def load_shift_schedule_latest(json,source):
    print("Data ingestion starting for alpl_shift_schedule")
    #json_obj = getJsonFile(json,source)
    partsLists = []
    cursor = con.cursor()
    # cursor.execute('select alpl_part_id from  alpl_uat.alpl_parts_master ')
    # res=cursor.fetchall()
    # partsLists = [i[0] for i in res]
    ctu = 0
    cti = 0
    p = 0
    errors = 0
    q = None
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'



    columMap ={'alpl_emp_id':'PERNR',
              'emp_name':'ZENAME',
              'unit_id':'ZWERKS'
    }

    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_uat.alpl_shift_schedule ('
            i = 0
            month = item.get('ZMONTH',None)
            year =  item.get('ZYEAR',None)
            shiftCode = item.get('ZBSHIFT',None)
            days = calendar.monthrange(int(year),int(month))[1]
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

                    if (table_column == 'unit_id'):
                        value = getUnitId(value)

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""

                    dataset = dataset + ',' + validate_string(value, table_column)
                i = 1
            for day in range(days):
                q = query
                d = dataset
                dayNum = None
                if (len(str(day + 1)) != 2):
                    dayNum = '0' + str(day + 1)
                else:
                    dayNum = str(day + 1)

                dayShift = 'Z' + dayNum
                shift = item.get(dayShift,None)
                value = getShiftId(shift,shiftCode)

                if (type(value) in (int, float)):
                    value = str(value)
                if (value == None):
                    value = ""

                startDate = str(year)+'-'+str(month)+'-'+str(dayNum)
                startDate = validate_string(startDate, 'startDate')
                endDate = startDate
                shiftId = validate_string(value, 'shiftId')

                q = q + ',' + 'start_date,end_date,shift_id)'
                d = d + ',' +str(startDate)+','+str(endDate)+','+str(shiftId)
                d = d + ')'
                totalQuery = q + d
                #print(totalQuery)
                cursor.execute(totalQuery)
        except Exception as e:
            print(e, i)
            print(query)
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
                errorQuery = errorQuery + validate_string('alpl_shift_scheduler', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery  + str(validate_string(e, 'tab')) + ')'
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

    print("Data ingestion completed. No of rows processed for alpl_shift_scheduler:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)
    print("errors :", errors)

def salary(json,source):
    print("Data ingestion starting for alpl_hr_salary_maintenence")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    column_map = {'emp_id': 'ALPLREFID',
    'emp_name': 'EMPNAME',
    'role_id': 'ROLE',
    'role_name' : 'ROLE_NAME',
    'unit_id': 'UNIT',
    'unit_name': 'UNIT_NAME',
    'dept_no': 'DEPARTMENT',
    'dept_name': 'DEPT_NAME',
    'salary': 'SALARY',
    'sub_group': 'SUBGROUP'
                  }
    uct = 0
    ict = 0
    empIdQuery = 'SELECT emp_id FROM alpl_uat.alpl_hr_salary_maintenance'
    cursor.execute(empIdQuery)
    result = cursor.fetchall()
    empIdList = [i[0] for i in result]
    for i, item in enumerate(json):

        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_hr_salary_maintenance ('
        updateQuery = 'UPDATE alpl_uat.alpl_hr_salary_maintenance SET '
        i = 0
        flag = 0
        valueID = None

        q = None

        try:
            for table_column, json_column in column_map.items():
                if (flag == 1) or (table_column == 'emp_id' and (int(item.get(json_column, None)) in empIdList)):
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

                        if (table_column == 'unit_id'):
                            value = getUnitId(value)

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

                        if (table_column == 'unit_id'):
                            value = getUnitId(value)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where emp_id = ' + str(valueID)
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                #print(q)
                cursor.execute(updateQuery)
                uct = uct+1
            else:
                q = totalQuery
                #print(q)
                cursor.execute(totalQuery)
                ict = ict + 1
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
                errorQuery = errorQuery + validate_string('alpl_hr_salary_maintenence', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_hr_salary_maintenence:", p)
    print("inserted :",ict)
    print("uodated :",uct)
    print("errors :", errors)
    print("connection closed successfully")


def godown_stock(json,source):
    print("Data ingestion starting for alpl_godown_stock_trans")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    p = 0
    errors = 0
    column_map = {
    'rcl_cust_id': 'RCL_CustID',
    'location_id': 'LocationID',
    'grade' : 'Grade',
    'alpl_godown_stock_id': 'ALPLRefId',
    'msl': 'MSL',
    'stock_level': 'StockLevel',
    'stock_value': 'StockValue',
    'capacity': 'Capacity',
    'remarks': 'Remarks',
    'delete': 'Deleted',
    'created_date_time': 'CreatedDateTime',
    'stock_date_time': 'StockDateTime'
                  }
    uct = 0
    ict = 0
    empIdQuery = 'select concat(rcl_cust_id,location_id,grade) from alpl_uat.alpl_godown_stock_trans'
    cursor.execute(empIdQuery)
    result = cursor.fetchall()
    stockList = [i[0] for i in result]
    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_uat.alpl_godown_stock_trans ('
        updateQuery = 'UPDATE alpl_uat.alpl_godown_stock_trans SET '
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

        try:
            for table_column, json_column in column_map.items():
                if (table_column == 'rcl_cust_id'):
                    custIdCheck = item.get(json_column, None)
                    if (table_column == 'rcl_cust_id'):
                        custId = custIdCheck
                        if (type(custId) in (int, float)):
                            custId = str(custId)

                        if (custId == None):
                            custId = ""
                    custId = validate_string(custId, table_column)


                if (table_column == 'location_id'):
                    locId = item.get(json_column, None)
                if (table_column == 'grade'):
                    gradeCheck = item.get(json_column, None)
                    grade=gradeCheck
                    if (type(grade) in (int, float)):
                        grade = str(grade)
                    if (grade == None):
                        grade = ""
                    grade = validate_string(grade, table_column)
                    checkValue = str(custIdCheck) + str(locId) + str(gradeCheck)

                if (flag == 1) or (table_column == 'alpl_godown_stock_id' and ((checkValue) in stockList)):
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

                        if (table_column == 'delete'):
                            if (value == 'False'):
                                value = 0
                            elif (value == 'True'):
                                value = 1
                            else:
                                value = None

                        if (type(value) in (int, float)):
                            value = str(value)

                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    i = 1
                elif(table_column != 'rcl_cust_id' and table_column != 'grade' and table_column != 'location_id' ):
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

                        if (table_column == 'delete'):
                            if(value == 'False'):
                                value = 0
                            elif(value == 'True'):
                                value = 1
                            else:
                                value = None



                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1

            dataset = dataset + ','+str(custId)+','+str(grade)+','+str(locId)+')'
            query = query + ',rcl_cust_id,grade,location_id)'

            updateQuery = updateQuery + ' where rcl_cust_id = ' + str(custId) + ' and location_id = '+ str(locId) + ' and grade= ' +  str(grade)
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                #print(q)
                cursor.execute(updateQuery)
                uct = uct+1
            else:
                q = totalQuery
                #print(q)
                cursor.execute(totalQuery)
                ict = ict + 1
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
                errorQuery = errorQuery + validate_string('alpl_godown_stock', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_godown_stock:", p)
    print("inserted :",ict)
    print("upated :",uct)
    print("errors :", errors)
    print("connection closed successfully")

def non_working_drivers(json,source):
    print("Data ingestion starting for alpl_non_working_drivers")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    p = 0
    column_map = {
    'driver_id': 'DRIVER_ID',
    'unit_id': 'UNIT',
    'unit_name':'UNIT_NAME',
    'driver_name' : 'DRIVER_NAME',
    'sd_amount': 'SD_MOUNT',
    'net_amount': 'NET_AMOUNT',
    'date' : 'DATE',
    'status': 'STATUS'
                  }
    uct = 0
    ict = 0
    driverIdQuery = 'select driver_id from alpl_uat.alpl_non_working_drivers'
    cursor.execute(driverIdQuery)
    result = cursor.fetchall()
    driverList = [i[0] for i in result]
    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_uat.alpl_non_working_drivers ('
        updateQuery = 'UPDATE alpl_uat.alpl_non_working_drivers SET '
        i = 0
        flag = 0
        valueID = None
        q = None

        try:
            for table_column, json_column in column_map.items():
                if (flag == 1) or (table_column == 'driver_id' and (str(item.get(json_column, None)) in driverList)):
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

                        if (table_column == 'unit_id'):
                            value = getUnitId(value)


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

                        if (table_column == 'unit_id'):
                            value = getUnitId(value)


                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1

            dataset = dataset +')'
            query = query + ')'

            updateQuery = updateQuery + ' where driver_id = ' + str(valueID)
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                #print(q)
                cursor.execute(updateQuery)
                uct = uct+1
            else:
                q = totalQuery
                #print(q)
                cursor.execute(totalQuery)
                ict = ict + 1
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
                errorQuery = errorQuery + validate_string('alpl_non_working_drivers', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_non_working_drivers:", p)
    print("inserted :",ict)
    print("upated :",uct)
    print("errors :", errors)
    print("connection closed successfully")

def state_master(json,source):
    print("Data ingestion starting for alpl_non_working_drivers")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    p = 0
    column_map = {
    'alpl_state_id': 'ALPLRefId',
    'name': 'Name',
    'code':'Code',
    'country_id' : 'CountryID'
         }
    uct = 0
    ict = 0
    stateIdQuery = 'select alpl_state_id from alpl_uat.alpl_state_master'
    cursor.execute(stateIdQuery)
    result = cursor.fetchall()
    stateList = [i[0] for i in result]

    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_uat.alpl_state_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_state_master SET '
        i = 0
        flag = 0
        valueID = None
        q = None

        try:
            for table_column, json_column in column_map.items():
                if (flag == 1) or (table_column == 'alpl_state_id' and (int(item.get(json_column, None)) in stateList)):
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


                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1

            dataset = dataset +')'
            query = query + ')'

            updateQuery = updateQuery + ' where alpl_state_id = ' + str(valueID)
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                print(q)
                #cursor.execute(updateQuery)
                uct = uct+1
            else:
                q = totalQuery
                print(q)
                cursor.execute(totalQuery)
                ict = ict + 1
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
                errorQuery = errorQuery + validate_string('alpl_non_working_drivers', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_non_working_drivers:", p)
    print("inserted :",ict)
    print("upated :",uct)
    print("errors :", errors)
    print("connection closed successfully")

def load_monthly_target_master(json,source):
    print("Data ingestion starting for monthly_target_master")
    #json_obj = getJsonFile(json,source)
    partsLists = []
    cursor = con.cursor()
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'

    ctu = 0
    cti = 0
    p = 0

    columMap ={
        'crusher_location_id': 'CrusherLocationID',
        'unit_id':'LocationCode',
        'crusher_capacity':'crushercapacity',
        'cement_frieght':'cementfrieght',
        'effective_from_date':'EffectiveDate',
        'effective_to_date': 'EffectiveTo'
    }

    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_uat.alpl_monthly_target_master ('
            i = 0
            for table_column, json_column in columMap.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + validate_string_mining(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)

                    if (table_column == 'unit_id'):
                        value = getUnitId(value)

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string_mining(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            #print(totalQuery)
            # print(dataset)
            cursor.execute(totalQuery)
        except Exception as e:
            print(e, i)
            print(query)
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
                errorQuery = errorQuery + validate_string('monthly_target_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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

    print("Data ingestion completed. No of rows processed for monthly_target_master:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("errors :", errors)
    print("updated rows:", ctu)

def district_master(json,source):
    print("Data ingestion starting for alpl_district_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    column_map = {
    'alpl_district_id': 'ALPLRefId',
    'district_name': 'Name',
    'district_code':'Code',
    'state_id' : 'StateId',
    'rcl_district_code':'RCLDistCode'
         }
    uct = 0
    ict = 0
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    districtIdQuery = 'select alpl_district_id from alpl_uat.alpl_district_master'
    cursor.execute(districtIdQuery)
    result = cursor.fetchall()
    districtList = [i[0] for i in result]

    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_uat.alpl_district_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_district_master SET '
        i = 0
        flag = 0
        valueID = None
        q = None

        try:
            for table_column, json_column in column_map.items():
                if (flag == 1) or (table_column == 'alpl_district_id' and (int(item.get(json_column, None)) in districtList)):
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

                        if(table_column == 'state_id'):
                            value = getStateId(value)

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

                        if (table_column == 'state_id'):
                            value = getStateId(value)


                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1

            dataset = dataset +')'
            query = query + ')'

            updateQuery = updateQuery + ' where alpl_district_id = ' + str(valueID)
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                #print(q)
                cursor.execute(updateQuery)
                uct = uct+1
            else:
                q = totalQuery
                #print(q)
                cursor.execute(totalQuery)
                ict = ict + 1
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
                errorQuery = errorQuery + validate_string('alpl_district_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_district_master:", p)
    print("inserted :",ict)
    print("upated :",uct)
    print("errors :", errors)
    print("connection closed successfully")

def supplier_master(json,source):
    print("Data ingestion starting for alpl_supplier_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    column_map = {
    'alpl_supplier_id': 'SUPPLIER_ID',
    'supplier_name': 'SUPPLIER_NAME',
    'supplier_group':'GROUP'
         }
    uct = 0
    ict = 0
    districtIdQuery = 'select alpl_supplier_id from alpl_uat.alpl_supplier_master'
    cursor.execute(districtIdQuery)
    result = cursor.fetchall()
    supplierList = [i[0] for i in result]

    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_uat.alpl_supplier_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_supplier_master SET '
        i = 0
        flag = 0
        valueID = None
        q = None

        try:
            for table_column, json_column in column_map.items():
                if (flag == 1) or (table_column == 'alpl_supplier_id' and (int(item.get(json_column, None)) in supplierList)):
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




                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1

            dataset = dataset +')'
            query = query + ')'

            updateQuery = updateQuery + ' where alpl_supplier_id = ' + str(valueID)
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                #print(q)
                cursor.execute(updateQuery)
                uct = uct+1
            else:
                q = totalQuery
                #print(q)
                cursor.execute(totalQuery)
                ict = ict + 1
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
                errorQuery = errorQuery + validate_string('alpl_supplier_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_supplier_master:", p)
    print("inserted :",ict)
    print("upated :",uct)
    print("errors :", errors)
    print("connection closed successfully")

def external_indent(json,source):
    print("Data ingestion starting for external_indent")
    #json_obj = getJsonFile(json,source)
    partsLists = []
    cursor = con.cursor()
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'

    cursor.execute('SELECT alpl_asset_model_id,asset_model_id FROM alpl_uat.alpl_asset_model')
    result = cursor.fetchall()
    assetList = [i[0] for i in result]
    assetModelIdList = [i[1] for i in result]

    dictAssetModel = {}
    length = len(assetModelIdList)
    for x in range(length):
        dictAssetModel[assetList[x]] = assetModelIdList[x]

    ctu = 0
    cti = 0
    p = 0

    columMap ={
        'order_id': 'ORDERID',
        'indent_issued_date':'PSTNG_DATE',
        'unit_id':'UNITID',
        'indent_issued':'INDENT_ISSUED',
        'indent_price':'AMOUNT',
        'vehicle_number': 'VECHICLE_NO',
        'asset_model_id': 'VECHICLE_MODEL'
    }

    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_uat.alpl_diesel_indent_external ('
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

                    if (table_column == 'unit_id'):
                        value = getUnitId(value)
                    if(table_column == 'asset_model_id'):
                        value = dictAssetModel.get(int(value))

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            #print(totalQuery)
            # print(dataset)
            cursor.execute(totalQuery)
            cti = cti+1
        except Exception as e:
            print(e, i)
            print(query)
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
                errorQuery = errorQuery + validate_string('alpl_diesel_indent_external', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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

    print("Data ingestion completed. No of rows processed for alpl_diesel_indent_external:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)
    print("errors :", errors)

def internal_indent(json,source):
    print("Data ingestion starting for internal_indent")
    #json_obj = getJsonFile(json,source)
    partsLists = []
    cursor = con.cursor()
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'

    cursor.execute('SELECT alpl_asset_model_id,asset_model_id FROM alpl_uat.alpl_asset_model')
    result = cursor.fetchall()
    assetList = [i[0] for i in result]
    assetModelIdList = [i[1] for i in result]

    dictAssetModel = {}
    length = len(assetModelIdList)
    for x in range(length):
        dictAssetModel[assetList[x]] = assetModelIdList[x]

    ctu = 0
    cti = 0
    p = 0

    columMap ={
        'order_id': 'ORDER',
        'indent_issued_date':'INDENT_DATE',
        'unit_id':'UNITID',
        'indent_issued':'INDENT_ISSUED',
        'indent_price':'INDENT_PRICE',
        'vehicle_number': 'VECHICLE_NO',
        'asset_model_id': 'VECHICLE_MODEL'
    }

    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_uat.alpl_diesel_indent_internal ('
            i = 0
            q = None
            for table_column, json_column in columMap.items():
                print('____________________________')
                print(table_column)
                print(item.get(json_column, None))
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

                    if (table_column == 'unit_id'):
                        value = getUnitId(value)
                    if(table_column == 'asset_model_id'):
                        if (value != '' or value != None):
                            value = dictAssetModel.get(int(value))
                        else:
                            print('value:',value)
                            value = None
                            print('value:', value)

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            #print(totalQuery)
            # print(dataset)
            cursor.execute(totalQuery)
            cti = cti+1
        except Exception as e:
            print(e, i)
            print(query)
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
                errorQuery = errorQuery + validate_string('alpl_diesel_indent_internal', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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

    print("Data ingestion completed. No of rows processed for alpl_diesel_indent_internal:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)
    print("errors :", errors)

def tyres_trans(json,source):
    print("Data ingestion starting for tyres_master")
    columMap = {
        'alpl_tyre_no':'TYRE_NO',
        'potion_tyre':'POTION_TYRE',
        'fitment_date':'FITMENT_DATE',
        'cfitment_date':'CFITMENT_DATE',
        'removal_date':'REMOVAL_DATE',
        'tyre_purchase_date':'TYRE_PURCH_DATE',
        'truck_number':'TRUCK_NUMBER',
        'trip_id':'TRIP_ID',
        'target_kms':'TARGET_KMS',
        'tyres_status':'TYRE_STATUS',
        'cost':'COST',
        'grn_date':'GRNDATE',
        'unit_id':'UNIT_NO'
    }

    cursor = con.cursor()
    cursor.execute('SELECT alpl_tyre_no from alpl_uat.alpl_tyre_trans')
    result = cursor.fetchall()
    tyreList = [i[0] for i in result]

    ctu = 0
    cti= 0
    p = 0
    errors = 0
    src = 'logistics'
    orgId = 1
    if ("MINING" in json_obj[source]):
        src = 'mining'
        orgId = 2

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_tyre_trans ('
        updateQuery = 'UPDATE alpl_uat.alpl_tyre_trans SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'alpl_tyre_no' and (
                        item.get(json_column, None) in tyreList)):
                    if flag == 0:
                        val = item.get(json_column, None)
                        if (type(val) in (int, float)):
                            val = str(val)
                        if (val == None):
                            val = ""

                        valueID = validate_string(val, table_column)
                        flag = 1
                    if (i == 0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)

                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'unit_id'):
                            value = getUnitId(value)


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


                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + validate_string(value, table_column)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'unit_id'):
                            value = getUnitId(value)


                        if (value == None):
                                value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + ',' + validate_string(value, table_column)


                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where alpl_tyre_no=' + valueID
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
                cti = cti+1
        except Exception as e:
            #print(e)
            #print(q)
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
                errorQuery = errorQuery + validate_string('alpl_tyre_trans', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
            p = p+1
    con.close()
    print("inserted rows:",cti)
    print("updated rows:",ctu)
    print("errors :", errors)
    print("connection tyre_trans closed successfully and no of rows inserted:",p)

def tyres_master(json,source):
    print("Data ingestion starting for tyres_master")
    columMap = {
        'alpl_tyre_no':'TYRE_NO',
        'tyre_type':'TYRE_TYPE',
        'tyre_model':'TYRE_MODEL',
        'manufacture':'MANUFACTURE',
        'tyre_purchase_date':'TYRE_PURCH_DATE',
        'tyre_war_valid_date':'TYRE_WAR_VALID_DATE',
        'tyre_size':'TYRE_SIZE',
        'tyre_reter_pat_type':'TYRE_RETRE_PAT_TYPE',
        'pattern_tyre':'PATTERN_TYRE',
        'tyre_status':'TYRE_STATUS',
        'tyre_act_scrap':'TYRE_ACT_SCRA',
        'tyre_scrap_date':'TYRE_SCRAP_DATE',
        'unit_id':'UNIT_NO',
        'rdate':'RDATE',
        'target_kms':'TARGET_KMS',
        'tyre_cost':'TYRE_COST',
        'store_location':'STORE_LOC',
        'tyre_kms':'TYRE_KMS'
    }

    cursor = con.cursor()
    cursor.execute('SELECT alpl_tyre_no from alpl_uat.alpl_tyre_master')
    result = cursor.fetchall()
    tyreList = [i[0] for i in result]

    ctu = 0
    cti= 0
    p = 0
    errors = 0
    src = 'logistics'
    orgId = 1
    if ("MINING" in json_obj[source]):
        src = 'mining'
        orgId = 2

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_tyre_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_tyre_master SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'alpl_tyre_no' and (
                        item.get(json_column, None) in tyreList)):
                    if flag == 0:
                        val = item.get(json_column, None)
                        if (type(val) in (int, float)):
                            val = str(val)
                        if (val == None):
                            val = ""

                        valueID = validate_string(val, table_column)
                        flag = 1
                    if (i == 0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)

                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'unit_id'):
                            value = getUnitId(value)


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


                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + validate_string(value, table_column)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'unit_id'):
                            value = getUnitId(value)


                        if (value == None):
                                value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + ',' + validate_string(value, table_column)


                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where alpl_tyre_no=' + valueID
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
                cti = cti+1
        except Exception as e:
            print(e)
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
                errorQuery = errorQuery + validate_string('alpl_tyre_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
            p = p+1
    con.close()
    print("inserted rows:",cti)
    print("updated rows:",ctu)
    print("errors :", errors)
    print("connection tyres_master closed successfully and no of rows modified:",p)


def runDataIngestion(json, func,source):
    starttime = time.time()
    procs = []
    l = len(json)
    print("rows : ", l)
    chunk = round(l / 8)
    # load_alpl_recoveries('x','y',json)

    # instantiating process with arguments
    if chunk > 0:
        for i in range(0, 8):
            print('Started with chunk', i * chunk, chunk * (i + 1))
            proc = Process(target=func, args=(json[i * chunk:chunk * (i + 1)],source))
            procs.append(proc)
            proc.start()

        # complete the processes
        for proc in procs:
            proc.join()
    else:
        func(json,source)

    print('Job completed in {} seconds'.format(time.time() - starttime))


if __name__ == "__main__":  # confirms that the code is under main function
    #json_obj = getJsonFile(json,source)
    source = str(json_obj)
    source = source[2:8]
    data = None
    if(source == 'Source'):
        data = 'data'
    elif(source == 'SOURCE'):
        data = 'DATA'
    json = json_obj[data]
    runDataIngestion(json, load_shift_schedule_latest,source)
