from datetime import datetime
import Util
import MultiProc
from LoggingUtil import logger



filename=''

org_id=2
driverListQuery = 'SELECT alpl_driver_reference_id from alpl_driver_list_master where org_id = '+ str(org_id)


def get_driver_id_map(cursor):
    cursor.execute('SELECT alpl_driver_reference_id,alpl_driver_id from alpl_driver_list_master where org_id='+str(org_id))
    dr_result=cursor.fetchall()
    driverid_map = dict(dr_result)
    return driverid_map

def validate_string(val, val1):
    val=str(val)
    if val.replace('.', '', 1).isdigit():
        return val
    elif val1 == 'recovery_type' or val1 == 'address':
        pos = -1
        pos = val.find("'", 0, len(val))
        if(pos >=0):
            val = val[:pos]+'\''+val[pos:]
            logger.info(val)
            return 'E'+"'" + val + "'"
        return "'" + val + "'"

    elif val1 == 'recovery_date' or val1 == 'exemption_date' or val1 == 'paid_date' or val1=='refund_date' or val1=='salary_date' or val1=='sd_date':
        if val == '0000-00-00':
            return 'NULL'
        elif val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'
    elif (val1 == 'to_date' or val1 == 'from_date' or val1 == 'health_checkup_date' or val1 == 'medi_claim_date' or val1 == 'driver_resuming_duty_date' 
          or val1 == 'driver_duty_end_date' or val1 == 'driver_next_resuming_duty_date' or val1 == 'driver_created_date_time' 
          or val1 == 'driver_job_start_date' or val1 == 'driver_license_date_of_issue' or val1 == 'driver_license_expiry_date' 
          or val1 == 'exit_date' or val1 == 'effective_date' or  val1 == 'driver_stage_start_date' or val1 == 'driver_stage_end_date' 
          or val1 == 'driver_job_start_date' or val1 == 'driver_license_date_of_issue' or val1 == 'driver_license_expiry_date' or val1 == 'exit_date' 
          or val1=='onhold_date' or val1=='release_date' or val1=='installment_created_date'):
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val1 == 'payslip_generated_date':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val1 == 'driver_date_of_application' or val1 == 'driver_overall_verification_closure_date' or val1 == 'date_of_birth':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val != None and val1=="trip_id":
        if val.isnumeric():
            return "'" + val + "'"
        else:
            return 'NULL'
    elif val != None and val != "":
        if "'" in val:
            return "'" + val.replace("'","''") + "'"
        return "'" + val + "'"
    else:
        return 'NULL'


def validate_string_surities(val, val1):
    val=str(val)
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



def load_alpl_driver_list_master(json,source,con):
    logger.info("Data ingestion starting for alpl_driver_list_master")
    #json_obj = getJsonFile(json,source)
    #con = Util.conn
    logger.info(con)
    cursor = con.cursor()
    p=0
    icut = 0
    ucut = 0
    errors = 0
    orgId = 2
    src = 'mining'
    drivers_list=[]
    driverListQuery = 'SELECT alpl_driver_reference_id from alpl_driver_list_master where org_id = '+ str(org_id)
    cursor.execute(driverListQuery)
    result = cursor.fetchall()
    driverIdList = [i[0] for i in result]
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
              'date_of_birth':'DateofBirth',
              'introducer_id':'IntroducerID'
                }
    for i, item in enumerate(json):

        dataset='values ('
        insertQuery='INSERT INTO alpl_driver_list_master ('
        updateQuery='UPDATE alpl_driver_list_master SET '
        i=0
        flag=0
        wherevalue='0'
        driver_id = int(item.get('ALPL Reference ID',None))
        drivers_list.append(driver_id)
        q = None

        try:
            for table_column,json_column in columMap.items():

                if (flag==1) or (table_column=='alpl_driver_reference_id' and (driver_id in driverIdList)):
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
                            elif(orgId ==1 and value=='6'):
                                value=0

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
                            elif(orgId ==1 and value=='6'):
                                value=0
                        if (value == None):
                            value = ""
                        elif (type(value) in (int, float)):
                            value = str(value)

                        #driverListId = str(driverListId)
                        dataset=dataset+','+validate_string(value,table_column)
                    i=1
            dataset=dataset+')'
            insertQuery=insertQuery+')'
            updateQuery=updateQuery+' ,last_updated_time=now() where alpl_driver_reference_id='+str(wherevalue)+' and org_id = '+ str(orgId)
            totalQuery=insertQuery+dataset
            if flag:
                q = updateQuery
                cursor.execute(updateQuery)
                #logger.info(q)
                ucut = ucut + 1
            else:
                q = totalQuery
                #logger.info(q)
                cursor.execute(totalQuery)
                icut = icut + 1
        except Exception as e:
            logger.error(str(e))
            #logger.info(q)
            con.rollback()
            try:
                Util.insert_error_record('alpl_driver_list_master', source, item, q, str(e),filename,driver_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p=p+1
    
    cursor.execute(driverListQuery)
    result = cursor.fetchall()
    driverIdListH = [i[0] for i in result]
    logger.info([i for i in drivers_list if i not in driverIdListH])
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_driver_list_master:"+ str(p))
    logger.info("connection closed successfully")
    logger.info("update:"+ str(ucut))
    logger.info("insert:"+ str(icut))
    logger.info("errors :"+ str(errors))

def load_alpl_driver_duty_period(json,source,con):
    logger.info("Data ingestion starting for alpl_driver_duty_period")
    #json_obj = getJsonFile(json,source)
    #con = Util.conn
    cursor = con.cursor()
    cursor.execute("select distinct driver_pay_slipid from alpl_driver_duty_period")
    result = cursor.fetchall()
    payslips = [i[0] for i in result]
    driverid_map = get_driver_id_map(cursor)
    p = 0
    u=0
    errors = 0
    src = 'mining'

    column_map = {'alpl_driver_id': 'DriverID', 'driver_resuming_duty_date': 'Driver Resuming Duty Date',
                  'driver_duty_end_date': 'Driver Duty End Date',
                  'driver_next_resuming_duty_date': 'NextResuming Duty Date', 
                  'driver_pay_slipid': 'PaySlipID',
                  'driver_created_userid': 'CreatedUserID',
                  'driver_created_date_time': 'CreatedDateTime',
                   'driver_duties_as_of_date': 'DutiesasOfDate',
                  'driver_net_amount': 'NetAmount',
                  'driver_is_duty_considered': 'isDutyConsidered',
                  'percentage':'Percentage',
                  'trips_considered':'Trips_Considered',
                  'pay_slip_code':'PaySlipCode'}

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_driver_duty_period ('
        i = 0
        q = None
        payslip = int(item.get('PaySlipID', None))
        try:
            if payslip in payslips:
                update_query = 'update alpl_driver_duty_period set trips_considered='
                update_query = update_query+"'"+item.get('Trips_Considered', 'null')+"'"
                update_query = update_query + ', pay_slip_code='+"'"+item.get('PaySlipCode', 'null')+"'"
                update_query = update_query+' ,driver_duties_as_of_date='+str(item.get('DutiesasOfDate', 'null'))
                dc = item.get('isDutyConsidered',None)
                value='0'
                if dc != None and dc == 'True':
                    value='1'
                update_query = update_query + ', driver_is_duty_considered='+value
                update_query = update_query +', last_updated_time=now() where driver_pay_slipid='+str(payslip)+';'
                cursor.execute(update_query)
                con.commit()
                u=u+1
            else:
                for table_column, json_column in column_map.items():
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'alpl_driver_id'):
                            value = driverid_map.get(int(value))
                            
                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'alpl_driver_id'):
                            value = driverid_map.get(int(value))
                        if (table_column == 'driver_is_duty_considered'):
                            if (value == 'True'):
                                value = '1'
                            else:
                                value = '0'
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
                p=p+1

            trip = item.get('Trips_Considered',None)
            if trip!= None:
                t = [x.strip() for x in trip.split(',')]
                trips = ','.join(t)
                update_trip = 'update alpl_trip_data set trip_considered_for_paysheet='+"'"+'true'+"'"+' where alpl_trip_id in ('
                update_trip = update_trip + trips +')'
                #logger.info(update_trip)
                cursor.execute(update_trip)
            

        except Exception as e:
            logger.error(str(e)+str(i))
            #logger.info(totalQuery)
            #logger.info(dataset)
            con.rollback()
            try:
                pass
                Util.insert_error_record('alpl_driver_duty_period', source, item, q, str(e),filename,payslip)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            #p = p + 1
    #cursor.execute('CALL update_driver_timesheet()')
    con.close()
    logger.info("Data ingestion completed. No of rows inserted for alpl_driver_duty_period:"+str(p))
    logger.info("No of rows updated for alpl_driver_duty_period:"+str(u))    
    logger.info("No of rows errored for alpl_driver_duty_period:"+str(errors))  
    logger.info("connection closed successfully")


def load_alpl_driver_leaves_and_rest(json,source,con):
    logger.info("Data ingestion starting for alpl_driver_leaves_and_rest")
    #json_obj = getJsonFile(json,source)
    #con = Util.conn
    orgId = 2
    src = 'mining'
    cursor = con.cursor()
    driverid_map = get_driver_id_map(cursor)
    cursor.execute('select alpl_driver_id, max(date(timesheet_start_date)) from alpl_driver_timesheet where timesheet_end_date is null group by 1')
    ts_result=cursor.fetchall()
    ts_map=dict(ts_result)
    p = 0
    column_map = {'alpl_driver_id': 'DriverID', 'to_date': 'ToDate', 'from_date': 'FromDate', 'leave_type': 'LeaveType','leave_type_id':'LeaveTypeID','org_id':'' }
    cursor.execute("select id,leave_type from alpl_leave_type_master")
    lt_result=cursor.fetchall()
    lt_map=dict(lt_result)
    for i, item in enumerate(json):
        driver_id=item.get('DriverID',None)
        driverid = driverid_map.get(int(driver_id))
        enddate=validate_string(item.get('FromDate', None), 'from_date')
        ltype=int(item.get('LeaveTypeID',None))
        dataset = 'values ('
        query = 'INSERT INTO alpl_driver_leaves_and_rest ('
        i = 0
        q = None
        errors=0
        try:
            for table_column, json_column in column_map.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'alpl_driver_id'):
                        value = driverid_map.get(int(value))
                        if(value!=None and value in ts_map):
                            update_ts='update alpl_driver_timesheet set status='+"'"'Resting'+"'"+',timesheet_end_date='
                            end_date=item.get('FromDate', None)
                            end_date=validate_string(end_date, 'from_date')
                            update_ts=update_ts+end_date +' where alpl_driver_id='+str(value)
                            update_ts =update_ts+ ' and timesheet_end_date is null and timesheet_start_date='+"'"+str(ts_map.get(value))+"'"
                            update_ts = update_ts+' and timesheet_start_date<'+end_date
                            #cursor.execute(update_ts)
                    if (value == None):
                        value = ""
                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset = dataset + validate_string(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'alpl_driver_id'):
                        value = driverid_map.get(int(value))

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
            logger.error(str(e)+str(i))
            #logger.info(query)
            #logger.info(dataset)
            con.rollback()
            try:
                Util.insert_error_record('alpl_driver_leaves_and_rest', source, item, q, str(e),filename,driver_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            if driver_id!=None and ltype!=None and lt_map!=None:
                update_off_road(driverid,ltype,lt_map)
            #con.update_driver_timesheet()
            p = p + 1
    #cursor.execute('CALL update_driver_timesheet()')
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_driver_leaves_and_rest:"+str(p))
    logger.info("errors :"+str( errors))
    logger.info("connection closed successfully")


def load_alpl_driver_onboarding(json,source,con):
    logger.info("Data ingestion starting for alpl_driver_onboarding")
    #json_obj = getJsonFile(json,source)
    #logger.info(json_obj)
    #con = Util.conn
    onb_list=[]
    cursor = con.cursor()
    onbQuery="select distinct (driver_registration_id::text || '-' || driver_verification_stage_id::text) as onb from alpl_driver_transactional"
    cursor.execute(onbQuery)
    onb_result=cursor.fetchall()
    onb_list = [i[0] for i in onb_result]
    
    cursor.execute("select alpl_asset_model_id,asset_model_id from alpl_asset_model")
    am_res=cursor.fetchall()
    model_map = dict(am_res)
    errors = 0
    src = 'mining'
    orgId = 2
    p = 0
    u=0
    #cursor.execute('truncate table alpl_driver_transactional')
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
                  'driver_overall_verification_closure_date': 'OverallVerificationClosureDate',
                  'vehicle_model':'VehicleModelID'
                  #'age':'Age'
                  }

    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_driver_transactional ('
            i = 0
            value = None
            q = None
            regno = item.get('RegistrationID',None)
            if(str(item.get('RegistrationID'))+'-'+str(item.get('VerificationStageID')) in onb_list):
                pass
                update_q='update alpl_driver_transactional set'
                std=item.get('StageStartDate', None)
                end=item.get('StageEndDate', None)
                status=item.get('VerificationStatus', None)
                regid=item.get('RegistrationID', None)
                stageid=item.get('VerificationStageID', None)
                code=item.get('drivercode', None)
                model=item.get('VehicleModelID', None)
                update_q = update_q + ' driver_stage_start_date='+validate_string(std, 'driver_stage_start_date')+','
                update_q = update_q + ' driver_stage_end_date='+validate_string(end, 'driver_stage_end_date')+','
                update_q = update_q + ' driver_verification_status='+validate_string(status, 'driver_verification_status')+','
                update_q = update_q + ' vehicle_model='+validate_string(model_map.get(int(model),'NULL'), 'vehicle_model')+','
                update_q = update_q + ' driver_code='+validate_string(code, 'driver_code')+', last_updated_time=now() '
                update_q = update_q + ' where driver_registration_id='+str(regid)
                update_q = update_q + ' and driver_verification_stage_id='+str(stageid)
                cursor.execute(update_q)
                u=u+1
            else:
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
                        if(table_column=='vehicle_model'):
                            value = model_map.get(int(value),'NULL')
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
                #logger.info(i, "rows inserted")
                #logger.info(totalQuery)
        except Exception as e:
            logger.error(str(e)+str(i))
            #logger.info(query)

            con.rollback()
            try:
                pass
                Util.insert_error_record('alpl_driver_transactional', source, item, q, str(e),filename,regno)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_driver_onboarding:"+str(p))
    logger.info("Updated :"+str(u))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")


def load_alpl_driver_sureties_master(json,source,con):
    logger.info("Data ingestion starting for alpl_sureties_master")
    #json_obj = getJsonFile(json,source)
    #con = Util.conn
    cursor = con.cursor()
    cursor.execute('SELECT alpl_driver_reference_id,registration_id FROM alpl_driver_list_master where org_id='+str(org_id))
    reg_result=cursor.fetchall()
    driverReg_map=dict(reg_result)
    drivers=set()
    if(len(json)<=150):
        for item in json:
            drivers.add(driverReg_map.get(int(item.get('DriverID'))))
        if(len(drivers)>1):
            dql_q = "delete from alpl_driver_sureties_master where registration_id in("
            drvs=','.join(str(v) for v in drivers)
            dql_q = dql_q+drvs+")"
            Util.delete_rows(dql_q)
        elif len(drivers)==1:
            dql_q = "delete from alpl_driver_sureties_master where registration_id="
            dql_q=dql_q+str(drivers[0])
            Util.delete_rows(dql_q)

    errors = 0
    src = 'logistics'
    if (source=="MINING"):
        src = 'mining'
    column_map = {'surety_registration_id': 'SuretyDriverID', 'effective_date': 'EffectiveDate',
                  'registration_id': 'DriverID'}
    p = 0
    #cursor.execute('TRUNCATE TABLE alpl_driver_sureties_master')
    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_driver_sureties_master ('
        i = 0
        q = None
        driver_id=item.get('DriverID',None)
        try:
            for table_column, json_column in column_map.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if(table_column == 'registration_id'):
                        value = driverReg_map.get(int(value))
                    if(table_column == 'surety_registration_id'):
                        value = driverReg_map.get(int(value))
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (table_column == 'surety_registration_id' and value == None):
                        logger.info('driver id does not exist: '+str(item.get(json_column, None)))
                    if(value == None):
                        value = ""
                    dataset = dataset + validate_string_surities(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)
                    if(table_column == 'registration_id'):
                        value = driverReg_map.get(int(value))
                    if(table_column == 'surety_registration_id'):
                        value = driverReg_map.get(int(value))
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (table_column == 'surety_registration_id' and value == None):
                        logger.info('driver id does not exist: '+str(item.get(json_column, None)))
                    if(value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string_surities(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            # logger.info(totalQuery)
            # logger.info(dataset)
            cursor.execute(totalQuery)
        except Exception as e:
            logger.error(str(e)+str(i))
            #logger.info(dataset)
            con.rollback()
            try:
                Util.insert_error_record('alpl_driver_sureties_master', source, item, q, str(e),filename,driver_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_surities_master:"+str(p))
    logger.info("errors :"+str( errors))
    logger.info("connection closed successfully")


def load_alpl_driver_vehicle_mapping(json,source, con):
    logger.info("Data ingestion starting for alpl_driver_vehicle_mapping")
    #json_obj = getJsonFile(json,source)
    #con = Util.conn
    cursor = con.cursor()
    driverid_map = get_driver_id_map(cursor)
    drivers=set()
    if(len(json)<=600):
        for item in json:
            driver=driverid_map.get(int(item.get('driverid')))
            if driver != None:
                drivers.add(driver)
        if(len(drivers)>1):
            dql_q = "delete from alpl_driver_vehicle_mapping where alpl_driver_id in("
            drvs=','.join(str(v) for v in drivers)
            dql_q = dql_q+drvs+")"
            Util.delete_rows(dql_q)
        elif len(drivers)==1:
            dql_q = "delete from alpl_driver_vehicle_mapping where alpl_driver_id="
            driver=list(drivers)
            dql_q=dql_q+str(driver[0])
            Util.delete_rows(dql_q)
    src = 'mining'
    errors = 0
    #cursor.execute('TRUNCATE TABLE alpl_driver_vehicle_mapping')
    p = 0
    column_map = {'alpl_driver_id': 'driverid', 'mapping_type': 'MappingType',
                  'vehicle_registration_number': 'VehicleNo', 'vehicle_model': 'VehicleModel'}
    for i, item in enumerate(json):
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_driver_vehicle_mapping ('
            i = 0
            q = None
            driver_id=item.get('driverid',None)
            for table_column, json_column in column_map.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'alpl_driver_id'):
                        value = driverid_map.get(int(value))
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (table_column == 'alpl_driver_id' and value == None):
                        logger.info('driver id does not exist: '+str(item.get(json_column, None)))
                    if (value == None):
                        value = ""
                    dataset = dataset + validate_string(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'alpl_driver_id'):
                        value = driverid_map.get(int(value))
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (table_column == 'alpl_driver_id' and value == None):
                        logger.info('driver id does not exist: '+str(item.get(json_column, None)))
                    if (value == None):
                        value = ""
                    dataset = dataset + ',' + validate_string(value, table_column)

                i = 1
            dataset = dataset + ')'
            query = query + ')'
            totalQuery = query + dataset
            cursor.execute(totalQuery)
        except Exception as e:
            logger.error(str(e)+str(i))
            #logger.error(query)
            #logger.info(dataset)
            con.rollback()
            try:
                Util.insert_error_record('alpl_driver_vehicle_mapping', source, item, q, str(e),filename,driver_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_driver_vehicle_mapping:"+str(p))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")


def load_alpl_recoveries(json,source,con):
    logger.info("Data ingestion starting for alpl_recoveries")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    src = 'logistics'
    errors = 0
    if (source=="MINING"):
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
                'recovery_date':'REC_DATE',
                'recovery_description':'REC_DESC',
                'paid_gl':'PAID_GL',
                'gl_code':'RECOVER_GL',
                'sd_amount':'SD_AMOUNT',
                'sd_date':'SD_DATE',
                'salary_amount':'SAL_AMOUNT',
                'salary_date':'SAL_DATE',
                'refund_amount':'REFUND_AMOUNT',
                'refund_date':'REFUND_DATE'
                }

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_recoveries ('
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
            #logger.info(totalQuery)
            q = totalQuery
            cursor.execute(totalQuery)
        except Exception as e:
            logger.info(e,+str(i))
            #logger.info(query)
            #logger.info(dataset)
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
                logger.info(errorQuery)
                #cursor.execute(errorQuery)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_recoveries:"+str( p))
    logger.info("errors :"+str( errors))
    logger.info("connection closed successfully")


def load_driver_medi_claim(json,source,con):
    logger.info("Data ingestion starting for alpl_driver_medi_claim")
    # json_obj = getJsonFile(json,source)
    #con = Util.conn
    cursor = con.cursor()
    driverid_map = get_driver_id_map(cursor)
    p = 0
    q=None
    orgId = 2
    errors=0
    src = 'mining'

    columMap = {'alpl_driver_id':'DriverID',
                'health_checkup_date':'HealthCheckupDate',
                'medi_claim_date':'MediClaimDate',
                'org_id':'',
                'health_status':'Healthstatus'}

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_driver_medi_claim ('
        i = 0
        value = None
        driver_id=item.get('DriverID',None)
        try:
            for table_column, json_column in columMap.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'alpl_driver_id'):
                        value = driverid_map.get(int(value))
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + validate_string(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'alpl_driver_id'):
                        value = driverid_map.get(int(value))
                    elif (table_column == 'org_id'):
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
            #logger.info(totalQuery)
        except Exception as e:
            logger.error(str(e)+str(i))
            con.rollback()
            try:
                Util.insert_error_record('alpl_driver_medi_claim', source, item, q, str(e),filename,driver_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            # logger.info("data inserted successfully")
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_driver_medi_claim:"+str(p))
    logger.info("errors :"+str( errors))
    logger.info("connection closed successfully")

def driver_installments(json, source,con):
    logger.info("Data ingestion starting for alpl_driver_installments")
    # json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'
    p = 0
    errors = 0
    column_map = {
        'driver_id': 'DriverID',
        'alpl_trip_id': 'TripID',
        'recovery_type': 'RecoveryType',
        'installment_number':'InstallmentNo',
        'installment_amount':'InstallmentAmount',
        'payslip_id': 'PayslipID',
        'payslip_generated_date': 'Payslipgenerateddate',
        'installment_created_date':'InstallmentCreatedDate',
        'is_active':'isActive'

    }
    uct = 0
    ict = 0
    empIdQuery = 'select concat(driver_id,alpl_trip_id,recovery_type,installment_number) from alpl_driver_installments'
    cursor.execute(empIdQuery)
    result = cursor.fetchall()
    stockList = [i[0] for i in result]

    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_driver_installments ('
        updateQuery = 'UPDATE alpl_driver_installments SET '
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

        dr_id = item.get('DriverID', None)
        if (dr_id == ''):
            dr_id = ''
        else:
            dr_id = str(dr_id)

        trip_id = item.get('TripID', None)
        if (trip_id == ''):
            trip_id = ''
        else:
            trip_id = str(trip_id)

        rec = item.get('RecoveryType', None)
        if (rec == ''):
            rec = ''
        else:
            rec = str(rec)

        ins = item.get('InstallmentNo', None)
        if (ins == ''):
            ins = ''
        else:
            ins = str(ins)

        checkValue = dr_id+trip_id+rec+ins


        try:
            for table_column, json_column in column_map.items():

                if (flag == 1) or ( table_column=='driver_id' and ((checkValue) in stockList)):
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
                        if(table_column=='is_active'):
                            if value=="1":
                                value='True'
                            else:
                                value='False'
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
                        if(table_column=='is_active'):
                            if value=="1":
                                value='True'
                            else:
                                value='False'
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1
                else:
                    if (i == 0):
                        query = query + table_column
                        value = item.get(json_column, None)

                        if (type(value) in (int, float)):
                            value = str(value)
                        if(table_column=='is_active'):
                            if value=="1":
                                value='True'
                            else:
                                value='False'
                        if (value == None):
                            value = ""
                        dataset = dataset + validate_string(value, table_column)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if (type(value) in (int, float)):
                            value = str(value)
                        if(table_column=='is_active'):
                            if value=="1":
                                value='True'
                            else:
                                value='False'
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1

            updateQuery = updateQuery + ' where driver_id = ' + str(dr_id) + ' and alpl_trip_id = ' + str(trip_id) + ' and recovery_type= ' +"'"+ str(rec)+"'"+' and installment_number='+str(ins)

            if (flag == 1):
                q = updateQuery
                #print(updateQuery)
                cursor.execute(updateQuery)
                uct = uct + 1

            else:
                dataset = dataset+ ')'
                query= query+')'
                query = query+dataset
                q=query
                #print(query)
                cursor.execute(query)
                ict = ict+1

        except Exception as e:
            logger.info(str(e) + str(i))
            logger.info(q)
            con.rollback()

            try:
                Util.insert_error_record('alpl_driver_installments', source, item, q, str(e), filename)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_driver_installments:"+ str(p))
    logger.info("inserted :"+str(ict))
    logger.info("updated :"+ str(uct))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")

def load_driver_onhold_txn(json,source,con):
    logger.info("Data ingestion starting for alpl_driver_onhold_txn")
    # json_obj = getJsonFile(json,source)
    #con = Util.conn
    onh_list=[]
    cursor = con.cursor()
    driverid_map = get_driver_id_map(cursor)
    ohn_q="select alpl_driver_reference_id || '-'||to_char(onhold_date,'YYYY-fmMM-fmDD HH12:MI:SS AM') dr from alpl_driver_onhold_txn oh, alpl_driver_list_master l where l.alpl_driver_id=oh.alpl_driver_id"
    cursor.execute(ohn_q)
    onh_result=cursor.fetchall()
    onh_list = [i[0] for i in onh_result]
    p = 0
    q=None
    orgId = 1
    errors=0
    u=0
    src = 'logistics'
    if ("MINING" in source):
        orgId = 2
        src = 'mining'

    columMap = {'alpl_driver_id':'driverid',
                'onhold_date':'OnHoldDateTime',
                'release_date':'ReleasedDateTime',
                'is_onhold':''}

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_driver_onhold_txn ('
        i = 0
        value = None
        driver_id=item.get('driverid',None)
        try:
            if(str(item.get('driverid'))+'-'+str(item.get('OnHoldDateTime')) in onh_list):
                onhdate=item.get('OnHoldDateTime')
                rlsdate=item.get('ReleasedDateTime',None)
                isonhold='false'
                if(rlsdate ==''):
                    isonhold='true'
                update_onh='update alpl_driver_onhold_txn set'
                update_onh = update_onh+ ' release_date='+validate_string(rlsdate, 'release_date')+','
                update_onh = update_onh +' is_onhold='+"'"+isonhold+"'"
                update_onh = update_onh +' where alpl_driver_id='+str(driverid_map.get(int(item.get('driverid'))))
                update_onh = update_onh +' and onhold_date='+validate_string(onhdate,'onhold_date')
                cursor.execute(update_onh)
                u=u+1
            else:
                for table_column, json_column in columMap.items():
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'alpl_driver_id'):
                            value = driverid_map.get(int(value))
                        if(table_column=='is_onhold'):
                            if(item.get('ReleasedDateTime',None)==''):
                                value='true'
                            else:
                                value='false'
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'alpl_driver_id'):
                            value = driverid_map.get(int(value))
                        if(table_column=='is_onhold'):
                            if(item.get('ReleasedDateTime',None)==''):
                                value='true'
                            else:
                                value='false'
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
                #logger.info(totalQuery)
        except Exception as e:
            logger.error(str(e)+ str(i))
            con.rollback()
            try:
                Util.insert_error_record('alpl_driver_onhold_txn', source, item, q, str(e),filename,driver_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            # logger.info("data inserted successfully")
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_driver_onhold_txn: "+ str(p))
    logger.info("Updated : "+ str(u))
    logger.info("errors : "+ str(errors))
    logger.info("connection closed successfully")


def load_alpl_driver_timesheet(json,source,con):
    logger.info("Data ingestion starting for alpl_driver_timesheet")
    #con = Util.conn
    cursor = con.cursor()
    driverid_map = get_driver_id_map(cursor)
    p = 0
    errors = 0
    src = 'logistics'
    if (source=="MINING"):
        src = 'mining'
    column_map = {'alpl_driver_id': 'DriverID', 
                  'timesheet_start_date': 'Driver Resuming Duty Date',
                  'status':''
                  }

    for i, item in enumerate(json):
        dataset = 'values ('
        #query = 'INSERT INTO alpl_driver_duty_period ('
        query= 'INSERT INTO alpl_driver_timesheet('
        i = 0
        q = None
        driver_id=item.get('DriverID',None)
        try:
            for table_column, json_column in column_map.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'alpl_driver_id'):
                        value = driverid_map.get(int(value))
                    if (table_column == 'status'):
                        value = 'On Duty'
                    if (value == None):
                        value = ""
                    if (type(value) in (int, float)):
                        value = str(value)
                    dataset = dataset + validate_string(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)
                    if (table_column == 'alpl_driver_id'):
                        value = driverid_map.get(int(value))
                    if (table_column == 'status'):
                        value = 'On Duty'
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
            logger.error(str(e)+str(i))
            #logger.error(query)
            #logger.info(dataset)
            con.rollback()
            try:
                Util.insert_error_record('alpl_driver_timesheet', source, item, q, str(e),filename,driver_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_driver_timesheet:"+ str(p))
    logger.info("connection closed successfully")


def update_off_road(driver,l_type,lt_map):
    try:
        status="LACK OF DRIVER"
        if l_type!=0 and l_type!=4:
            status = 'Driver-'+lt_map.get(l_type)
        elif l_type==4:
            status='LACK OF ORDER'
        con = Util.engine.raw_connection()
        cursor=con.cursor()
        truck_trip="""select td.asset_reg_number,trip_id,driver_id,is_completed from alpl_trip_data td
        where driver_id="""+str(driver)+"""and trip_creation_time=(select max(tt.trip_creation_time) from alpl_trip_data tt
        where tt.driver_id=td.driver_id )
        and td.asset_reg_number not in(select tr.asset_reg_number from alpl_trip_data tr
        where tr.trip_creation_time=(select max(tt.trip_creation_time) from alpl_trip_data tt
        where tt.asset_reg_number=tr.asset_reg_number)
        and tr.asset_reg_number=td.asset_reg_number
        and tr.is_completed=0)"""
        cursor.execute(truck_trip)
        result=cursor.fetchall()
        if len(result)>=1:
            truck=result[0][0]
            trip=result[0][1]
            iscomp = result[0][3]
            if iscomp==1:
                insert_qry = 'insert into alpl_asset_telemetry(asset_reg_number,trip_id,message_timestamp,status,created_time,last_updated_time) values'
                insert_qry = insert_qry+'('+"'"+truck+"',"+str(trip)+",0"+",'"+status+"',"+"now(),now())"
                cursor.execute("select tel.status from alpl_asset_telemetry tel where tel.asset_reg_number="+"'"+truck+"' and trip_id="+str(trip)+" and tel.created_time=(select max(created_time) from alpl_asset_telemetry where trip_id=tel.trip_id)")
                res = cursor.fetchall()
                if len(res)==0:
                    pass
                    cursor.execute(insert_qry)
                elif len(res)>=0:
                    prev_status = res[0][0]
                    if prev_status!=status:
                        pass
                        cursor.execute(insert_qry)
    except Exception as exp:
        logger.error(str(exp))
        con.rollback()
        con.close()
    else:
        con.commit()
        con.close()

def main(file_name):  # confirms that the code is under main function
    
    try:
        global filename
        filename = file_name
        json_obj = Util.getJsonFile(Util.mining_portal_bucket,file_name)
        logger.info("Processing file: "+file_name)
        source = str(json_obj)
        ch = source.find('[')
        dataKey = source[ch-7:ch-3]
        source = source[2:8]
        stored_proc=None
        if ("MINING" in json_obj[source]):
            org_id = 2
        data = None
        sourcetype = 'PORTAL'
        if(source == 'Source'):
            data = 'data'
            dest_bucket = Util.mining_processed_bucket
        elif(source == 'SOURCE'):
            data = 'DATA'
            sourcetype='SAP'
            dest_bucket = Util.mining_processed_bucket
        jsonData = json_obj[dataKey]
        src = json_obj[source]
        func =None
        if(src=="MINING_DRIVER_MASTER"):
            func = load_alpl_driver_list_master
        elif(src=="MINING_DRIVER_VEHICLEMAPPING_TRANS"):
            func=load_alpl_driver_vehicle_mapping
        elif(src=="MINING_DRIVER_ONBOARD_TRANS"):
            func=load_alpl_driver_onboarding
        elif(src=="MINING_DRIVER_REST_LEAVES_TRANS"):
            pass
            func = load_alpl_driver_leaves_and_rest
        elif(src=="MINING_DRIVER_HEALTH_MEDI_TRANS"):
            pass
            func = load_driver_medi_claim
        if func != None:
            MultiProc.runDataIngestion(jsonData, func,src)
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