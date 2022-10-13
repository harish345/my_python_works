import psycopg2;
import os, json;
from datetime import datetime;
import multiprocessing as mp;
import time;
from multiprocessing import Process;

# read JSON file which is in the next parent folder
file = os.path.abspath('D:/json_files') + "/act.json"
json_data = open(file).read()
json_obj = json.loads(json_data)
legList = []
locationIdList = []
locationTypeIdList=[]
vendorIdList=[]
trips={}
con = psycopg2.connect(user="postgres",
                       password="admin",
                       host="34.93.62.36",
                       port="5432",
                       database="alpl_test")
cursor = con.cursor()

cursor.execute('SELECT job_card_id from alpl_uat.alpl_job_card_transaction')
result = cursor.fetchall()
jobCardListTrans = [i[0] for i in result]

# cursor.execute('SELECT job_card_id from alpl_uat.alpl_activity_trans_duplicate')
# result = cursor.fetchall()
# jobCardListActivityTrans = [i[0] for i in result]

def validate_string_job_card(val, val1):
    if val.replace('.', '', 1).isdigit():
        if val != None and val != "":
            return val
        else:
            return 'NULL'

    if ((val.find("'", 0, len(val)) >= 0)):
        val = val.replace('\'', '\\\'')
        return 'E' + "'" + val + "'"

    elif val1 == 'job_card_create_date' or val1 == 'job_card_end_date' or val1 == 'activity_creation_date' or val1 == 'activity_end_date' or val1 == 'activity_approved_date':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'

    elif val1 == 'job_card_create_time' or val1 == 'job_card_end_time' or val1 == 'activity_creation_time' or val1 == 'activity_end_time':
        if (val == '24:00:00'):
            return "'" + '23:59:59' + "'"
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%H:%M:%S').strftime('%H:%M:%S') + "'"
        else:
            return 'NULL'

    elif val1 == 'activity_creation_end_time' or val1 == 'activity_creation_date_time':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y/%m/%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'

    elif val != None and val != "":
        return "'" + val + "'"
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

    elif val1 == 'job_card_create_date' or val1 == 'job_card_end_date' or val1 == 'activity_creation_date' or val1 == 'activity_end_date' or val1 == 'activity_approved_date':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'

    elif val1 == 'job_card_create_time' or val1 == 'job_card_end_time' or val1 == 'activity_creation_time' or val1 == 'activity_end_time':
        if (val == '24:00:00'):
            return "'" + '23:59:59' + "'"
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%H:%M:%S').strftime('%H:%M:%S') + "'"
        else:
            return 'NULL'

    elif val1 == 'activity_creation_end_time' or val1 == 'activity_creation_date_time':
        if (val == '0000-00-00 00:00:00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'

    elif val != None and val != "":
        return "'" + val + "'"
    else:
        return 'NULL'


def load_activity_master(json,source):

    print("Data ingestion starting for alpl_activity_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    ctu = 0
    cti = 0
    errors = 0
    src = 'logistics'
    #     # if ("MINING" in json_obj[source]):
    #     #     src = 'mining'
    columMap = {'job_card_id': 'JOBCARDID',
                'alpl_reference_id': 'ALPLREFID',
                'activity_id': 'ACTIVITYID',
                'truck_type': 'TRUCKTYPE',
                'activity_type': 'ACTIVITYTYPE',
                'activity_name': 'ACTIVITYNAME',
                'activity_category': 'ACTIVITYCATEGORY',
                'activity_sla': 'ACTIVITYSLA'}

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_activity_master ('
        updateQuery = 'UPDATE alpl_uat.alpl_activity_master SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        jobCardValue = None
        refId = None
        actId = None
        check = 0
        whereQuery = None
        insertCheck = 0
        updateCheck = 0
        # maxId = None
        # IdQuery = "SELECT MAX(id) FROM alpl_uat.alpl_activity_master"
        # cursor.execute(IdQuery)
        # count = cursor.fetchone()[0]
        # if(count == None):
        #     maxId = 0
        # else:
        #     maxId = count + 1

        try:
            for table_column, json_column in columMap.items():

                if (table_column == 'job_card_id'):
                    jobCardValue = item.get(json_column, None)

                    if (type(jobCardValue) in (int, float)):
                        jobCardValue = str(jobCardValue)

                    if (jobCardValue == None):
                        jobCardValue = ""
                    jobCardValue = validate_string(jobCardValue, table_column)


                if (table_column == 'alpl_reference_id'):
                    refId = item.get(json_column, None)

                    if (type(refId) in (int, float)):
                        refId = str(refId)

                    if (refId == None):
                        refId = ""
                    refId = validate_string(refId, table_column)


                if (table_column == 'activity_id'):
                    actId = item.get(json_column, None)

                    actId = str(actId)

                    if (actId == None):
                        actId = ""
                    actId = validate_string(actId, table_column)
                    check = 1

                if (check == 1):
                    checkQuery = "SELECT COUNT(*) FROM alpl_uat.alpl_activity_master"
                    whereQuery = " WHERE (job_card_id = " + jobCardValue + " or job_card_id is NULL)  AND (alpl_reference_id = " + refId + " or alpl_reference_id is NULL) AND (activity_id = " + actId + " or activity_id is NULL)"
                    checkQuery = checkQuery + whereQuery
                    cursor.execute(checkQuery)
                    count = cursor.fetchone()[0]
                    check = 0

                    if (count == 0):
                        insertCheck = 1
                    else:
                        updateCheck = 1

                if (flag == 1) or (updateCheck == 1):

                    if (flag == 0):
                        value = item.get(json_column, None)
                        if (i == 0):
                            updateQuery = updateQuery + table_column
                            value = item.get(json_column, None)
                            if (table_column == 'job_card_id'):
                                value = jobCardValue
                                if (value == None):
                                    value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        valueID = validate_string(value, table_column)
                        flag = 1

                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1

                elif (insertCheck == 1):

                    if i == 0:
                        query = query + table_column
                        if (table_column == 'activity_id'):
                            value = actId
                        # if (value == None) or (value == 'NULL'):
                        #     value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + value

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if (value == None) or (value == 'NULL'):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1
            dataset = dataset + ', ' + jobCardValue + ' ,' + refId + ')'
            query = query + ',job_card_id,alpl_reference_id' + ')'
            updateQuery = updateQuery + ' ' + whereQuery
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
                errorQuery = errorQuery + validate_string('alpl_activity_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_activity_master:", p)
    print("updated rows :", ctu)
    print("inserted rows :", cti)
    print("errors :",errors)
    print("connection closed successfully")


def load_activity_trans(json,source):
    print("Data ingestion starting for alpl_activity_trans")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    errors = 0
    if ("MINING" in json_obj[source]):
        src = 'mining'

    p = 0
    ctu = 0
    cti = 0

    # cursor.execute('SELECT max(alpl_activity_trans_id) from alpl_uat.alpl_activity_trans')
    # result = cursor.fetchall()[0]
    # if (result[0] == None):
    #     activity_trans_id = 0
    # else:
    #     activity_trans_id = result[0]

    cursor.execute('SELECT location,parts_location_master_id FROM alpl_uat.alpl_parts_location_master')
    result = cursor.fetchall()
    locationList = [i[0] for i in result]
    locationIdList = [i[1] for i in result]

    dictLoc = {}
    length = len(locationList)
    for x in range(length):
        dictLoc[locationList[x]] = locationIdList[x]


    column_map = {'job_card_id': 'JOBCARDID',
                  'alpl_reference_id': 'ALPLREFID',
                  'activity_name': 'ACTIVITYNAME',
                  'truck_no': 'TRUCKNO',
                  'trip_id': 'TRIPID',
                  'activity_creation_date': 'ACTCRT_DATE',
                  'activity_creation_time': 'ACTCRT_TIME',
                  'activity_end_date': 'ACTEND_DATE',
                  'activity_end_time': 'ACTEND_TIME',
                  'supervisor_number': 'DRIVER_NUMBER',
                  'supervisor_name': 'DRIVER_NAME',
                  'activity_status': 'ACTVSTATUS',
                  'name_of_spart': 'NAMEOF_SPART',
                  'spare_part_id': 'SPARE_PARTS_ID',
                  'quantity_of_spare': 'QNTYOFSPARE',
                  'activity_approved_by': 'ACTVTYAPPROVEDBY',
                  'activity_approved_date': 'ACTVITYAPPROVEDDATE',
                  'activity_creation_date_time': '',
                  'activity_creation_end_time': '',
                  'activity_dept': 'ACTVTY_DEPT',
                  'spare_used': 'SPARESUSED',
                  'indent_id': 'INDENTID',
                  'role_id': 'ROLE',
                  'role_name':'ROLE_NAME',
                  'batch_id':'BATCH_ID',
                  'storage_location':'STORAGELOCATION',
                  'group_value':'GROUP',
                  'activity':'ACTIVITY',
                  'operation':'OPERATION',
                  'activity_txt':'ACTIVITY_TXT',
                  'planned_hrs':'PLND_HRS',
                  'actual_hrs':'ACTUAL_HRS'}



    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_activity_trans ('
        updateQuery = 'UPDATE alpl_uat.alpl_activity_trans SET '
        i = 0
        creationDate = ''
        creationTime = ''
        endDate = ''
        endTime = ''
        flag = 0
        valueID = '0'
        jobCardValue = None
        refId = None
        actId = None
        check = 0
        insertCheck = 0
        updateCheck = 0
        q = None
        spare_name = item.get('NAMEOF_SPART', None)
        if (spare_name == ''):
            spare_name = ''
        else:
            spare_name = str(spare_name)

        spare_id = item.get('SPARE_PARTS_ID', None)
        if (spare_id == ''):
            spare_id = ''
        else:
            spare_id = str(spare_id)

        spare_name = validate_string(spare_name, 'table_column')
        spare_id = validate_string(spare_id, 'table_column')

        try:
            for table_column, json_column in column_map.items():
                if (table_column == 'job_card_id'):
                    jobCardValue = item.get(json_column, None)

                    if (type(jobCardValue) in (int, float)):
                        jobCardValue = str(jobCardValue)

                    if (jobCardValue == None):
                        jobCardValue = ""
                    jobCardValue = validate_string(jobCardValue, table_column)


                if (table_column == 'alpl_reference_id'):
                    refId = item.get(json_column, None)

                    if (type(refId) in (int, float)):
                        refId = str(refId)

                    if (refId == None):
                        refId = ""
                    refId = validate_string(refId, table_column)


                if (table_column == 'activity_name'):
                    actId = item.get(json_column, None)

                    actId = str(actId)

                    if (actId == None):
                        actId = ""
                    actId = validate_string(actId, table_column)
                    check = 1

                if (check == 1):
                    checkQuery = "SELECT COUNT(*) FROM alpl_uat.alpl_activity_trans"
                    whereQuery = " WHERE (job_card_id = " + jobCardValue + " or job_card_id is NULL) AND (alpl_reference_id = " + str(refId) + " or alpl_reference_id is NULL) AND (activity_name = " + str(actId) + " or activity_name is NULL) "
                    whereQuery = whereQuery + " AND name_of_spart = "+str(spare_name) + " AND spare_part_id = "+str(spare_id)
                    checkQuery = checkQuery + whereQuery
                    cursor.execute(checkQuery)
                    count = cursor.fetchone()[0]
                    check = 0
                    if (count == 0):
                        insertCheck = 1
                    else:
                        updateCheck = 1


                if (flag == 1) or (updateCheck == 1):
                    if (flag == 0):
                        value = item.get(json_column, None)

                        if (i == 0):
                            updateQuery = updateQuery + table_column
                            value = item.get(json_column, None)

                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)

                        flag = 1
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        # if (table_column == 'mechanic_id'):
                        #     empIdQuery = 'SELECT emp_id FROM alpl_uat.alpl_employee_master_new WHERE alpl_emp_id = '
                        #     temp = item.get(json_column, None)
                        #     if (temp == None):
                        #         value = ""
                        #     else:
                        #         empIdQuery = empIdQuery + temp
                        #         cursor.execute(empIdQuery)
                        #         empId = cursor.fetchone()
                        #         if (empId == None):
                        #             value = ""
                        #         else:
                        #             value = empId[0]
                        #
                        #
                        # elif (table_column == 'trip_id'):
                        #     tripIdQuery = 'SELECT trip_id FROM alpl_uat.alpl_trip_data WHERE alpl_trip_id = '
                        #
                        #     temp = item.get(json_column, None)
                        #     if (temp == None):
                        #         value = ""
                        #     else:
                        #         tripIdQuery = tripIdQuery + str(temp)
                        #         cursor.execute(tripIdQuery)
                        #
                        #         tripId = cursor.fetchone()
                        #         if (tripId == None):
                        #             value = ""
                        #         else:
                        #             value = tripId[0]



                        if (table_column == 'activity_creation_date'):
                            creationDate = creationDate + value
                        elif (table_column == 'activity_end_date'):
                            endDate = endDate + value
                        elif (table_column == 'activity_creation_time'):
                            if(value == '24:00:00'):
                                value = '23:59:59'
                            creationTime = creationTime + value

                        elif (table_column == 'activity_end_time'):
                            if (value == '24:00:00'):
                                value = '23:59:59'
                            endTime = endTime + value

                        elif (table_column == 'activity_creation_date_time'):
                            value = ""
                            value = str(value + creationDate + ' ' + creationTime)

                        elif (table_column == 'activity_creation_end_time'):
                            value = ""
                            value = str(value + endDate + ' ' + endTime)

                        elif(table_column == 'storage_location'):
                            value = dictLoc.get(int(value))

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1
                elif(insertCheck == 1):


                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)




                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset +  validate_string(value, table_column)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)

                        # if (table_column == 'mechanic_id'):
                        #     empIdQuery = 'SELECT emp_id FROM alpl_uat.alpl_employee_master_new WHERE alpl_emp_id = '
                        #     temp = item.get(json_column, None)
                        #     if (temp == None):
                        #         value = ""
                        #     else:
                        #         empIdQuery = empIdQuery + temp
                        #         cursor.execute(empIdQuery)
                        #         empId = cursor.fetchone()
                        #         if (empId == None):
                        #             value = ""
                        #         else:
                        #             value = empId[0]
                        #
                        #
                        # elif (table_column == 'trip_id'):
                        #     tripIdQuery = 'SELECT trip_id FROM alpl_uat.alpl_trip_data WHERE alpl_trip_id = '
                        #     temp = item.get(json_column, None)
                        #
                        #     if (temp == None):
                        #         value = ""
                        #     else:
                        #         tripIdQuery = tripIdQuery + str(temp)
                        #
                        #         cursor.execute(tripIdQuery)
                        #         tripId = cursor.fetchone()
                        #         if (tripId == None):
                        #             value = ""
                        #         else:
                        #             value = tripId[0]


                        if (table_column == 'activity_creation_date'):
                            creationDate = creationDate + value
                        elif (table_column == 'activity_end_date'):
                            endDate = endDate + value
                        elif (table_column == 'activity_creation_time'):
                            if (value == '24:00:00'):
                                value = '23:59:59'
                            creationTime = creationTime + value

                        elif (table_column == 'activity_end_time'):
                            if (value == '24:00:00'):
                                value = '23:59:59'
                            endTime = endTime + value

                        if (table_column == 'activity_creation_date_time'):
                            value = ""
                            value = str(value + creationDate + ' ' + creationTime)

                        elif (table_column == 'activity_creation_end_time'):
                            value = ""
                            value = str(value + endDate + ' ' + endTime)

                        elif (table_column == 'storage_location'):
                            value = dictLoc.get(int(value))

                        if (type(value) in (int, float)):
                            value = str(value)
                        elif (value == None):
                            value = ""


                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1
            dataset = dataset + ', ' + jobCardValue + ' ,' + refId +')'
            query = query + ',job_card_id,alpl_reference_id' + ')'
            updateQuery = updateQuery + ' ' + whereQuery
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
                errorQuery = errorQuery + validate_string('alpl_activity_trans', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1

        else:
            p = p + 1
            con.commit()

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_activity_trans:", p)
    print("updated rows :",ctu)
    print("inserted rows :", cti)
    print("error rows :", errors)
    print("connection closed successfully")

def load_job_card_trans(json,source):
    print("Data ingestion starting for alpl_job_card_trans")
    #json_obj = getJsonFile(json,source)
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

    p = 0
    ctu = 0
    cti =0
    errors = 0
    column_map = {'job_card_id': 'JOBCARDID',
                  'unit_number': 'UNIT_NUMBER',
                  'type_of_maintenance': 'TYPE_OF_MAINTENANCE',
                  'job_card_status': 'JOBCARDSTATUS',
                  'jca_prescribed': 'JCA_PRESCRIBED',
                  'superviser_number': 'SUP_NUMBER',
                  'superviser_name': 'SUP_NAME',
                  'driver_number': 'DRIVER_NUMBER',
                  'driver_name': 'DRIVER_NAME',
                  'number_of_kms': 'NUMBER_KMS',
                  'deffects_observed': 'DEFECTS_OBSERVNS',
                  'lube_oil_consumption': 'LUBE_OIL_CONSUP',
                  'actual_hr_sla': 'ACTUAL_HR_SLA',
                  'planned_hr_sla': 'PLANNED_HR_SLA',
                  'deviation': 'DEVIATION',
                  'truck_number': 'TRUCKNO',
                  'ad_blue_consumption': 'AD_BLUE_CONSUP',
                  'tcdm_activity': 'TCDM_ACTVITY',
                  'job_card_create_date': 'JOBCARDODATE',
                  'job_card_create_time': 'JOBCARDOTIME',
                  'job_card_end_date': 'JOBCARD_EDATE',
                  'job_card_end_time': 'JOBCARD_ETIME'}



    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_job_card_transaction ('
        updateQuery = 'UPDATE alpl_uat.alpl_job_card_transaction SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None

        try:
            for table_column, json_column in column_map.items():
                if (flag == 1) or (table_column == 'job_card_id' and (
                        int(validate_string_job_card(item.get(json_column, None), table_column)) in jobCardListTrans)):
                    if flag == 0:
                        val = item.get(json_column, None)
                        if (type(val) in (int, float)):
                            val = str(val)
                        valueID = validate_string_job_card(val, table_column)
                        flag = 1
                    if (i == 0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string_job_card(value, table_column)

                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'unit_number' ):
                            if(value != 'ALPL'):
                                value = dictUnitc.get(int(value))
                            else:
                                value = ''

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string_job_card(value, table_column)


                    i = 1
                else:
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""

                        dataset = dataset + validate_string_job_card(value, table_column)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'unit_number'):
                            if (value != 'ALPL'):
                                value = dictUnitc.get(int(value))
                            else:
                                value = ''

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""

                        dataset = dataset + ',' + validate_string_job_card(value, table_column)

                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' where job_card_id=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                cti = cti +1
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
                errorQuery = errorQuery + validate_string('alpl_job_card_master', 'tab') + ',' + str(
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
            p = p + 1

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_job_Card_trans:", p)
    print("updated rows :",ctu)
    print("inserted rows :", cti)
    print("Error rows :",errors)
    print("connection closed successfully")


def load_job_card_master(json,source):
    print("Data ingestion starting for alpl_job_card_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0

    column_map = {'job_card': 'JOBCARC', 'activity': 'ACTVITY',
                  'job_card_id': 'JOBCARDID'}

    # try:
    # cursor.execute('DELETE FROM alpl_uat.alpl_job_card_master_duplicate_updated')
    # except Exception as e:
    # print(e)
    # else:
    # con.commit()
    # print("data deleted successfully")

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_uat.alpl_job_card_master_new ('
        i = 0
        flag = 0
        q = None
        try:
            for table_column, json_column in column_map.items():
                if i == 0:
                    query = query + table_column
                    dataset = dataset + validate_string_job_card(item.get(json_column, None), table_column)
                else:
                    query = query + ',' + table_column
                    dataset = dataset + ',' + validate_string_job_card(item.get(json_column, None), table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            q = totalQuery
            print(q)
            cursor.execute(totalQuery)
        except Exception as e:
            print(e, i)
            print(q)
            print(dataset)
        else:
            print("data inserted successfully")
            con.commit()
            p = p + 1

    con.close()
    print("Data ingestion completed. No of rows processed for alpl_job_Card_master:", p)
    print("connection closed successfully")

def activity_dummy(json,source):
    print("Data ingestion starting for alpl_activity_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_uat.alpl_unit_master')
    result = cursor.fetchall()
    unitCodeList = [i[0] for i in result]
    unitIdList = [i[1] for i in result]

    dictUnitc = {}
    length = len(unitCodeList)
    for x in range(length):
        dictUnitc[unitCodeList[x]] = unitIdList[x]

    cursor.execute('SELECT concat(plant,group_value,activity) from alpl_uat.alpl_activity_master_new')
    result = cursor.fetchall()
    actList = [i[0] for i in result]


    p = 0
    cti = 0
    ctu = 0
    column_map = {'plant': 'PLANT', 'group_value': 'GROUP','activity':'ACTIVITY','act_text':'ACTTEXT',
                  'hours':'HOURS','operation_key':'OPERATIONKEY','group_text':'GROUPTEXT',
                  'operation_key_txt':'OPERATIONKEY_TXT'
                  }

    # try:
    # cursor.execute('DELETE FROM alpl_uat.alpl_job_card_master_duplicate_updated')
    # except Exception as e:
    # print(e)
    # else:
    # con.commit()
    # print("data deleted successfully")

    for i, item in enumerate(json):
        dataset = 'values ('
        insertQuery = 'INSERT INTO alpl_uat.alpl_activity_master_new ('
        updateQuery = 'UPDATE alpl_uat.alpl_activity_master_new SET '
        i = 0
        flag = 0
        q = None
        valueID = None
        plant = item.get('PLANT', None)
        plant = dictUnitc.get(int(plant))
        if (plant == ''):
            value = ""
        group = item.get('GROUP', None)
        if (group == ''):
            value = ""
        activity = item.get('ACTIVITY', None)
        if (activity == ''):
            value = ""
        upcheck = str(plant)+str(group)+str(activity)

        try:
            for table_column, json_column in column_map.items():
                if (flag == 1) or ((table_column == 'plant') and (upcheck in actList)):
                    if flag == 0:
                        value = plant
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        valueID = ' where plant = '
                        valueID = valueID + str(validate_string(value, table_column))+ ' and group_value = '
                        value = group
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        valueID = valueID + str(validate_string(value, table_column))+ ' and activity = '
                        value = activity
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)
                        valueID = valueID + str(validate_string(value, table_column))
                    flag = 1
                    if i == 0:
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)

                        if (table_column == 'plant'):
                            value = dictUnitc.get(int(value))

                        if (value == None):
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
                        if (table_column == 'plant'):
                            value = dictUnitc.get(int(value))

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""

                        dataset = dataset + validate_string(value, table_column)



                    else:
                        insertQuery = insertQuery + ',' + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""

                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1
            dataset = dataset + ')'
            insertQuery = insertQuery + ')'
            updateQuery = updateQuery + str(valueID)
            totalQuery = insertQuery + dataset

            if flag:
                cursor.execute(updateQuery)
                ctu = ctu + 1
                #print(updateQuery)
            else:
                cursor.execute(totalQuery)
                cti = cti + 1
                #print(totalQuery)


        except Exception as e:
            print(e)
        else:
            con.commit()
            p = p+ 1
    con.close()

    print("Data ingestion completed. No of rows processed for alpl_activity_master:", p)
    print("connection closed successfully")
    print("inserted rows:", cti)
    print("updated rows:", ctu)




    con.close()
    print("Data ingestion completed. No of rows processed for alpl_activity_master_new:", p)
    print("connection closed successfully")


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
    runDataIngestion(json, load_activity_trans,source)

