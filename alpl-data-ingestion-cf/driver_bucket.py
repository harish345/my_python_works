import psycopg2;
import os, json;
from datetime import datetime

# read JSON file which is in the next parent folder
file = os.path.abspath('D:/json_files') + "/dr.json"
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
                       database="alpl_integration")

cursor = con.cursor()
cursor.execute('SELECT alpl_driver_id from alpl_dev_redesign.alpl_driver_list_master')
result=cursor.fetchall()
driverIdList=[i[0] for i in result]

# cursor.execute('SELECT alpl_driver_id from alpl_dev_redesign.alpl_driver_list_master')
# result=cursor.fetchall()
# driverIdList=[i[0] for i in result]

def validate_string(val, val1):
    if val.replace('.', '', 1).isdigit():
        return val
    elif val1 == 'recovery_type' or val1 == 'address':
        pos = -1
        pos = val.find("'", 0, len(val))
        if(pos >=0):
            val = val[:pos]+'\''+val[pos:]
            print(val)
            return 'E'+"'" + val + "'"
        return "'" + val + "'"

    elif val1 == 'recovery_date' or val1 == 'exemption_date' or val1 == 'recovery_date' or val1 == 'paid_date':
        if val == '0000-00-00':
            return 'NULL'
        elif val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'
    elif val1 == 'to_date' or val1 == 'from_date' or val1 == 'health_checkup_date' or val1 == 'medi_claim_date' or val1 == 'driver_resuming_duty_date' or val1 == 'driver_duty_end_date' or val1 == 'driver_next_resuming_duty_date' or val1 == 'driver_created_date_time' or val1 == 'driver_job_start_date' or val1 == 'driver_license_date_of_issue' or val1 == 'driver_license_expiry_date' or val1 == 'exit_date' or val1 == 'effective_date' or val1 == 'driver_stage_start_date' or val1 == 'driver_stage_end_date' or val1 == 'driver_job_start_date' or val1 == 'driver_license_date_of_issue' or val1 == 'driver_license_expiry_date' or val1 == 'exit_date':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val1 == 'driver_date_of_application' or val1 == 'driver_overall_verification_closure_date' or val1 == 'date_of_birth':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val != None and val != "":
        return "'" + val + "'"
    else:
        return 'NULL'


def validate_string_surities(val, val1):
    if val.replace('.', '', 1).isdigit():
        return val
    elif val1 == 'effective_date':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val != None and val != "":
        return "'" + val + "'"
    else:
        return 'NULL'



def load_alpl_driver_list_master(json,source):
    print("Data ingestion starting for alpl_driver_list_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p=0
    icut = 0
    ucut = 0
    orgId = 1
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        orgId = 2
        src = 'mining'

    driverListQuery = 'SELECT alpl_driver_reference_id from alpl_dev_redesign.alpl_driver_list_master where org_id = '+ str(orgId)
    cursor.execute(driverListQuery)
    result = cursor.fetchall()
    driverIdList = [i[0] for i in result]


    # cursor.execute('SELECT max(driver_list_master_id) from alpl_dev_redesign.alpl_driver_list_master')
    # result = cursor.fetchall()[0]
    # if (result[0] == None):
    #     driverListId = 0
    # else:
    #     driverListId = result[0]

    columMap={
              'alpl_driver_reference_id': 'ALPL Reference ID',
              'unit_number': 'Unit Number',
              'photo':'Driver Photo',
              #'driver_adhaar_card':'Adhaar Card',
              'driver_code':'DriverCode',
              'driver_job_start_date':'Job Start Date',
              'driver_leaves_entitled':'Number of leaves entitled',
              'driver_license_number':'Driver License Number',
              'driver_license_type':'Driver License Type(Heavy/Light vehicle)',
              'driver_license_date_of_issue':'Driver License Date Of Issue',
              'driver_license_expiry_date':'Driver License Expiry Date',
              'driver_license_place_of_issue':'Driver License Place Of Issue',
              'driver_name':'Driver Name',
              'driver_alternate_phone_number':'Driver Alternate Phone Number',
              'driver_phone_number':'Driver Phone Number',
              'exit_date':'Exit Date',
              'registration_id':'RegistrationID',
              'security_deposit':'Security Deposit',
              'driver_status':'Driver Status(Active/Inactive)',
              'reference_through':'ReferenceThrough',
              'exit_reason':'ExitReason',
              'address':'Address',
              'driver_home_lat':'Latitude',
              'driver_home_long':'Longitude',
              #'age':'Age',
              'org_id':'',
              'date_of_birth':'DateofBirth'
                }
    for i, item in enumerate(json):

        dataset='values ('
        insertQuery='INSERT INTO alpl_dev_redesign.alpl_driver_list_master ('
        updateQuery='UPDATE alpl_dev_redesign.alpl_driver_list_master SET '
        i=0
        flag=0
        wherevalue='0'
        driverList = int(item.get('ALPL Reference ID',None))
        q = None

        try:
            for table_column,json_column in columMap.items():

                if (flag==1) or (table_column=='alpl_driver_reference_id' and (driverList in driverIdList)):
                    if flag==0:
                        value = item.get(json_column,None)
                        if (value == None):
                            value = ""
                        elif (type(value) in (int, float)):
                            value = str(value)
                        wherevalue=validate_string(value,table_column)
                    flag=1
                    if i==0:
                        updateQuery=updateQuery+table_column
                        value = item.get(json_column,None)

                        if (value == None):
                            value = ""
                        elif (type(value) in (int, float)):
                            value = str(value)

                        updateQuery=updateQuery+'='+validate_string(value,table_column)
                    else:
                        updateQuery=updateQuery+','+table_column
                        value = item.get(json_column, None)

                        if (table_column == 'org_id'):
                            value = orgId

                        if (table_column == 'unit_number'):
                            if (orgId == 2):
                                if (value == '2'):
                                    value = 7
                                elif (value == '1'):
                                    value = 8

                        if (value == None):
                            value = ""
                        elif (type(value) in (int, float)):
                            value = str(value)
                        updateQuery=updateQuery+'='+validate_string(value,table_column)
                    i=1
                else:
                    if i==0:
                        insertQuery=insertQuery+table_column
                        value = item.get(json_column, None)
                        if (value == None):
                            value = ""
                        elif (type(value) in (int, float)):
                            value = str(value)
                        dataset=dataset+validate_string(value,table_column)
                    else:
                        insertQuery=insertQuery+','+table_column
                        value = item.get(json_column, None)

                        if (table_column == 'org_id'):
                            value = orgId

                        if (table_column == 'unit_number'):
                            if (orgId == 2):
                                if (value == '2'):
                                    value = 7
                                elif (value == '1'):
                                    value = 8
                        if (value == None):
                            value = ""
                        elif (type(value) in (int, float)):
                            value = str(value)

                        #driverListId = str(driverListId)
                        dataset=dataset+','+validate_string(value,table_column)
                    i=1
            dataset=dataset+')'
            insertQuery=insertQuery+')'
            updateQuery=updateQuery+' where alpl_driver_reference_id='+str(wherevalue)+' and org_id = '+ str(orgId)
            totalQuery=insertQuery+dataset
            if flag:
                q = updateQuery
                cursor.execute(updateQuery)
                #print(q)
                ucut = ucut + 1
            else:
                q = totalQuery
                #print(q)
                cursor.execute(totalQuery)
                icut = icut + 1
        except Exception as e:
            print(e)
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
                errorQuery = errorQuery + validate_string('alpl_driver_list_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_driver_list_master:", p)
    print("connection closed successfully")
    print("update:",ucut)
    print("insert:",icut)
    print("errors :", errors)

def load_alpl_driver_duty_period(json,source):
    print("Data ingestion starting for alpl_driver_duty_period")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    #cursor.execute('TRUNCATE TABLE alpl_driver_duty_period')
    # cursor.execute('SELECT max(alpl_driver_duty_period_id) from alpl_dev_redesign.alpl_driver_duty_period')
    # result = cursor.fetchall()[0]
    # if (result[0] == None):
    #     duty_period_id = 0
    # else:
    #     duty_period_id = result[0]
    column_map = {'alpl_driver_id': 'DriverID', 'driver_resuming_duty_date': 'Driver Resuming Duty Date',
                  'driver_duty_end_date': 'Driver Duty End Date',
                  'driver_next_resuming_duty_date': 'NextResuming Duty Date', 'driver_pay_slipid': 'PaySlipID',
                  'driver_created_userid': 'CreatedUserID',
                  'driver_created_date_time': 'CreatedDateTime', 'driver_duties_as_of_date': 'DutiesasOfDate',
                  'driver_net_amount': 'NetAmount',
                  'driver_is_duty_considered': 'isDutyConsidered',
                  'percentage':'Percentage',
                  #'alpl_driver_duty_period_id':''
                  }

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_driver_duty_period ('
        i = 0
        q = None
        try:
            for table_column, json_column in column_map.items():
                if i == 0:
                    query = query + table_column
                    dataset = dataset + validate_string(item.get(json_column, None), table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'driver_is_duty_considered'):
                        if (value == 'True'):
                            value = '1'
                        else:
                            value = '0'
                    # if(table_column == 'alpl_driver_duty_period_id'):
                    #     duty_period_id = duty_period_id + 1
                    #     value = duty_period_id
                    if (value == None):
                        value = ""
                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
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
                errorQuery = errorQuery + validate_string('alpl_driver_duty_period', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    cursor.execute('CALL update_driver_timesheet()')
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_driver_duty_period:", p)
    print("connection closed successfully")


def load_alpl_driver_leaves_and_rest(json,source):
    print("Data ingestion starting for alpl_driver_leaves_and_rest")
    #json_obj = getJsonFile(json,source)
    orgId = 1
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        orgId = 2
        src = 'mining'
    cursor = con.cursor()
    # cursor.execute('SELECT max(alpl_driver_leaves_and_rest_id) from alpl_dev_redesign.alpl_driver_leaves_and_rest')
    # result = cursor.fetchall()[0]
    # if (result[0] == None):
    #     leaves_and_rest_id = 0
    # else:
    #     leaves_and_rest_id = result[0]
    p = 0
    column_map = {'alpl_driver_id': 'DriverID', 'to_date': 'ToDate', 'from_date': 'FromDate', 'leave_type': 'LeaveType','org_id':''
                  }
    for i, item in enumerate(json):

        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_driver_leaves_and_rest ('
        i = 0
        q = None
        try:
            for table_column, json_column in column_map.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if (value == None):
                        value = ""
                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset = dataset + validate_string(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)



                    # if(table_column=='alpl_driver_leaves_and_rest_id'):
                    #     leaves_and_rest_id =leaves_and_rest_id + 1
                    #     value =leaves_and_rest_id
                    if(table_column == 'org_id'):
                        value = orgId

                    if (value == None):
                        value = ""
                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
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
                errorQuery = errorQuery + validate_string('alpl_driver_leaves_and_rest', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            #con.update_driver_timesheet()
            p = p + 1
    cursor.execute('CALL update_driver_timesheet()')
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_driver_leaves_and_rest:", p)
    print("errors :", errors)
    print("connection closed successfully")


def load_alpl_driver_onboarding(json,source):
    print("Data ingestion starting for alpl_driver_onboarding")
    #json_obj = getJsonFile(json,source)
    #print(json_obj)
    cursor = con.cursor()
    errors = 0
    src = 'logistics'
    orgId = 1
    if ("MINING" in json_obj[source]):
        orgId = 2
        src = 'mining'
    p = 0
    cursor.execute('truncate table alpl_driver_transactional')
    column_map = {'driver_registration_id': 'RegistrationID',
                  'alpl_unit_id':'LocationID',
                  'driver_code': 'drivercode',
                  'driver_name': 'DriverName',
                  'driver_date_of_application': 'DateofApplication',
                  'driver_verification_stage_id': 'VerificationStageID',
                  'driver_verification_stage': 'VerificationStage',
                  'driver_verification_status': 'VerificationStatus',
                  'driver_created_user_id': 'CreatedUserID',
                  'driver_stage_start_date': 'StageStartDate',
                  'driver_stage_end_date': 'StageEndDate',
                  'driver_rejection_reason_if_any': 'RejectionReasonifany',
                  'driver_number_of_installments': 'Number Of Installments',
                  'driver_overall_verification_closure_date': 'OverallVerificationClosureDate'
                  #'age':'Age'
                  }

    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_dev_redesign.alpl_driver_transactional ('
            i = 0
            value = None
            q = None
            for table_column, json_column in column_map.items():
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
                    if(table_column == 'alpl_unit_id'):
                        if(orgId == 2):
                            if(value == '2'):
                                value = 7
                            elif(value == '1'):
                                value = 8

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            cursor.execute(totalQuery)
            #print(i, "rows inserted")
            #print(totalQuery)
        except Exception as e:
            print(e, i)
            print(query)

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
                errorQuery = errorQuery + validate_string('alpl_driver_onboarding', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_driver_onboarding:", p)
    print("errors :", errors)
    print("connection closed successfully")


def load_alpl_driver_sureties_master(json,source):
    print("Data ingestion starting for alpl_sureties_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    errors = 0
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        src = 'mining'
    # cursor.execute('SELECT max(alpl_driver_sureties_master_id) from alpl_dev_redesign.alpl_driver_sureties_master')
    # result = cursor.fetchall()[0]
    # if (result[0] == None):
    #     sureties_master_id = 0
    # else:
    #     sureties_master_id = result[0]
    column_map = {'surety_registration_id': 'SuretyDriverID', 'effective_date': 'EffectiveDate',
                  'registration_id': 'DriverID'}
    p = 0
    #cursor.execute('TRUNCATE TABLE alpl_dev_redesign.alpl_driver_sureties_master')
    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_driver_sureties_master ('
        i = 0
        q = None
        try:
            for table_column, json_column in column_map.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if(table_column == 'surety_registration_id'):
                        regQuery = "SELECT registration_id FROM alpl_dev_redesign.alpl_driver_list_master where alpl_driver_id = "
                        regQuery = regQuery + value
                        cursor.execute(regQuery)
                        reg = cursor.fetchone()
                        if (reg == None):
                            value = ""
                        else:
                            value = reg[0]
                    if (type(value) in (int, float)):
                        value = str(value)
                    if(value == None):
                        value = ""
                    dataset = dataset + validate_string_surities(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'registration_id'):
                        regQuery = "SELECT registration_id FROM alpl_dev_redesign.alpl_driver_list_master where alpl_driver_id = "
                        regQuery = regQuery + value
                        cursor.execute(regQuery)
                        reg = cursor.fetchone()
                        if (reg == None):
                            value = ""
                        else:
                            value = reg[0]
                    # if(table_column == 'alpl_driver_sureties_master_id'):
                    #     sureties_master_id =sureties_master_id + 1
                    #     value = sureties_master_id
                    if (type(value) in (int, float)):
                        value = str(value)
                    if(value == None):
                        value = ""

                    dataset = dataset + ',' + validate_string_surities(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            # print(totalQuery)
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
                errorQuery = errorQuery + validate_string('alpl_surities_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_surities_master:", p)
    print("errors :", errors)
    print("connection closed successfully")


def load_alpl_driver_vehicle_mapping(json,source):
    print("Data ingestion starting for alpl_driver_vehicle_mapping")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    errors = 0
    if ("MINING" in json_obj[source]):
        src = 'mining'
    cursor.execute('TRUNCATE TABLE alpl_dev_redesign.alpl_driver_vehicle_mapping_new')
    p = 0
    column_map = {'alpl_driver_id': 'driverid', 'mapping_type': 'MappingType',
                  'vehicle_registration_number': 'VehicleNo', 'vehicle_model': 'VehicleModel'}
    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_dev_redesign.alpl_driver_vehicle_mapping_new ('
            i = 0
            q = None
            for table_column, json_column in column_map.items():
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
                errorQuery = errorQuery + validate_string('alpl_activity_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_driver_vehicle_mapping:", p)
    print("errors :", errors)
    print("connection closed successfully")


def load_alpl_recoveries(json,source):
    print("Data ingestion starting for alpl_recoveries")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    src = 'logistics'
    errors = 0
    if ("MINING" in json_obj[source]):
        src = 'mining'
    columMap = {'trip_id': 'TRIPID',
                 'driver_code': 'DRIVER_ID',
                'recovery_type': 'REC_TYPE',
                'recovery_amount': 'REC_AMOUNT',
                'exemption_amount': 'EXE_AMOUNT',
                'paid_amount': 'PAID_AMOUNT',
                'exemption_date':'EXE_DATE',
                'paid_date':'PAID_DATE',
                'driver_name':'DRIVER_NAME',
                'recovery_date':'REC_DATE'
                }

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_recoveries ('
        i = 0
        value = None
        q = None
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
            q = totalQuery
            #cursor.execute(totalQuery)
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
                errorQuery = errorQuery + validate_string('alpl_recoveries', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
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
    print("Data ingestion completed. No of rows processed for alpl_recoveries:", p)
    print("errors :", errors)
    print("connection closed successfully")


def driver_medi_claim(json,source):
    print("Data ingestion starting for alpl_driver_medi_claim")
    # json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    orgId = 1
    src = 'logistics'
    if ("MINING" in json_obj[source]):
        orgId = 2
        src = 'mining'

    columMap = {'alpl_driver_id':'DriverID',
                'health_checkup_date':'HealthCheckupDate',
                'medi_claim_date':'MediClaimDate',
                'org_id':''}

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_dev_redesign.alpl_driver_medi_claim ('
        i = 0
        value = None
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
                    if (table_column == 'org_id'):
                        value = orgId
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            cursor.execute(totalQuery)
            #print(totalQuery)
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
                errorQuery = errorQuery + validate_string('alpl_activity_master', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + "'" + str(e) + "'" + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            # print("data inserted successfully")
            con.commit()
            p = p + 1
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_driver_medi_claim:", p)
    print("errors :", errors)
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
    runDataIngestion(json, load_alpl_employee_attendance,source)

