import Util
from datetime import datetime
import MultiProc
from LoggingUtil import logger
import requests
import time

currentTime = datetime.now();
currentTimestamp = str(currentTime)
currentTimestamp = "'" + currentTimestamp[:currentTimestamp.find('.')] + "'"
currentTimestamp = 'now()'

filename = ''


def validate_string_job_card(val, val1):
    val=str(val)
    if val.replace('.', '', 1).isdigit():
        if val!=None and val1=='document_no':
            return "'"+val+"'"
        elif val != None and val != "":
            return val
        else:
            return 'NULL'

    if ((val.find("'", 0, len(val)) >= 0)):
        val = val.replace('\'', '\\\'')
        return 'E' + "'" + val + "'"

    elif val1=='final_confirmation':
        if val != None and val != "":
            return 'true'
        else:
            return 'false'

    elif val1 == 'job_card_create_date' or val1 == 'job_card_end_date' or val1 == 'activity_creation_date' or val1 == 'activity_end_date' or val1 == 'activity_approved_date' or val1 == 'issue_date' or val1=='document_date':
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


def load_job_card_trans(json, source, con):
    logger.info("Data ingestion starting for alpl_job_card_trans")
    # json_obj = getJsonFile(json,source)
    cursor = con.cursor()

    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'

    logger.info("check 1")
    cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_unit_master')
    result = cursor.fetchall()
    unitCodeList = [i[0] for i in result]
    unitIdList = [i[1] for i in result]

    dictUnitc = {}
    length = len(unitCodeList)
    for x in range(length):
        dictUnitc[unitCodeList[x]] = unitIdList[x]
    logger.info("check 2")
    jobCardListTrans = []
    cursor.execute('SELECT job_card_id from alpl_job_card_transaction')
    result = cursor.fetchall()
    jobCardListTrans = [i[0] for i in result]
    logger.info("check 3")

    p = 0
    ctu = 0
    cti = 0
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
        query = 'INSERT INTO alpl_job_card_transaction ('
        updateQuery = 'UPDATE alpl_job_card_transaction SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        data = {}
        jcid = item.get('JOBCARDID', None)
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

                        if (table_column == 'unit_number'):
                            if (value != 'ALPL'):
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
            updateQuery = updateQuery + ',last_updated_time=' + currentTimestamp + ' where job_card_id=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                cti = cti + 1
        except Exception as e:
            logger.info(str(e) + str(i))
            logger.info(q)
            con.rollback()
            try:
                Util.insert_error_record('alpl_job_card_trans', source, item, q, str(e), filename, jcid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1

        else:
            con.commit()
            p = p + 1
            activity_status = validate_string_job_card(item.get('JOBCARDSTATUS', None), 'job_card_status')
            if (activity_status == "'Order Completed'"):
                activity_update = "update alpl_activity_trans at set activity_status='Order Completed' where at.job_card_id=" + validate_string_job_card(item.get('JOBCARDID', None), 'job_card_id')
                cursor.execute(activity_update)

            status = validate_string_job_card(item.get('JOBCARDSTATUS', None), 'job_card_status')
            job_type = validate_string_job_card(item.get('TYPE_OF_MAINTENANCE', None), 'type_of_maintenance')
            if (job_type == "'JC04'" and status == "'Order Completed'"):
                updateTeleQuery = 'update alpl_asset_telemetry at set status=' + "'On Road'" + ' from (select * from alpl_asset_telemetry iat where iat.asset_reg_number=' + validate_string_job_card(
                    item.get('TRUCKNO', None),
                    'truck_number') + ' order by iat.created_time desc limit 1) as innerq where innerq.aat_id=at.aat_id and at.asset_reg_number=' + validate_string_job_card(
                    item.get('TRUCKNO', None), 'truck_number') + ' and at.status=' + "'ACCIDENT'"
                cursor.execute(updateTeleQuery)
            if (job_type == "'JC05'" and status == "'Order Completed'"):
                updateTeleQuery = 'update alpl_asset_telemetry at set status=' + "'On Road'" + ' from (select * from alpl_asset_telemetry iat where iat.asset_reg_number=' + validate_string_job_card(
                    item.get('TRUCKNO', None),
                    'truck_number') + ' order by iat.created_time desc limit 1) as innerq where innerq.aat_id=at.aat_id and at.asset_reg_number=' + validate_string_job_card(
                    item.get('TRUCKNO', None), 'truck_number') + ' and at.status=' + "'OBD'"
                cursor.execute(updateTeleQuery)
            con.commit()
            if (flag == 1):
                pass
            else:
                status = validate_string_job_card(item.get('JOBCARDSTATUS', None), 'job_card_status')
                if (status == "'Order Released'"):
                    data['jobId'] = int(validate_string_job_card(item.get('JOBCARDID', None), 'job_card_id'))
                    data['orgId'] = 1
                    auth_token = '86ebc511-0e80-40d2-92fa-1d123b2c98bc'
                    hed = {'Authorization': 'Bearer ' + auth_token}
                    requests.post("http://api.arunachala.cc/alplapp/jobcard/triggerWorkflow", params=data, headers=hed)

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_job_Card_trans:" + str(p))
    logger.info("updated rows :" + str(ctu))
    logger.info("inserted rows :" + str(cti))
    logger.info("Error rows :" + str(errors))
    logger.info("connection closed successfully")


def load_activity_trans(json, source, con):
    logger.info("Data ingestion starting for alpl_activity_trans")
    # json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    errors = 0
    if ("MINING" in source):
        src = 'mining'

    p = 0
    ctu = 0
    cti = 0

    cursor.execute('SELECT location,parts_location_master_id FROM alpl_parts_location_master')
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
                  'role_id': 'JOBKEY',
                  'role_name': 'JOBKEY_DESC',
                  'batch_id': 'BATCH_ID',
                  'storage_location': 'STORAGELOCATION',
                  'group_value': 'GROUP',
                  'activity': 'ACTIVITY',
                  'operation': 'OPERATION',
                  'activity_txt': 'ACTIVITY_TXT',
                  'planned_hrs': 'PLND_HRS',
                  'actual_hrs': 'ACTUAL_HRS',
                  'issue_date': 'ISSUEDATE',
                  'counter': 'COUNTER',
                  'final_confirmation': 'FCONFRM',
                  'issue_cost':'ISSUED_COST',
                  'duration':'DURATION'
                  }

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_activity_trans ('
        updateQuery = 'UPDATE alpl_activity_trans SET '
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
        check = 1
        insertCheck = 0
        updateCheck = 0
        q = None
        jcid = item.get('ALPLREFID', None)
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

        jobCardValue = item.get('JOBCARDID', None)
        if (jobCardValue == ''):
            jobCardValue = ''
        else:
            jobCardValue = str(jobCardValue)

        refId = item.get('ALPLREFID', None)
        if (refId == ''):
            refId = ''
        else:
            refId = str(refId)

        actId = item.get('ACTIVITYNAME', None)
        if (actId == ''):
            actId = ''
        else:
            actId = str(actId)

        supNum = item.get('DRIVER_NUMBER', None)
        if (supNum == ''):
            supNum = ''
        else:
            supNum = str(supNum)
        createDate = item.get('ACTCRT_DATE', None)
        if (createDate == ''):
            createDate = ''
        else:
            createDate = str(createDate)
        createTime = item.get('ACTCRT_TIME', None)
        if (createTime == ''):
            createTime = ''
        else:
            createTime = str(createTime)
        activityNum = item.get('ACTIVITY', None)
        if (activityNum == ''):
            activityNum = ''
        else:
            activityNum = str(activityNum)

        spare_name = validate_string_job_card(spare_name, 'table_column')
        spare_id = validate_string_job_card(spare_id, 'table_column')
        jobCardValue = validate_string_job_card(jobCardValue, 'table_column')
        refId = validate_string_job_card(refId, 'table_column')
        actId = validate_string_job_card(actId, 'table_column')
        supNum = validate_string_job_card(supNum, 'table_column')
        createDate = validate_string_job_card(createDate, 'table_column')
        createTime = validate_string_job_card(createTime, 'table_column')
        activityNum = validate_string_job_card(activityNum, 'table_column')

        try:
            for table_column, json_column in column_map.items():
                # if (table_column == 'job_card_id'):
                #     jobCardValue = item.get(json_column, None)
                #
                #     if (type(jobCardValue) in (int, float)):
                #         jobCardValue = str(jobCardValue)
                #
                #     if (jobCardValue == None):
                #         jobCardValue = ""
                #     jobCardValue = validate_string_job_card(jobCardValue, table_column)
                #
                #
                # if (table_column == 'alpl_reference_id'):
                #     refId = item.get(json_column, None)
                #
                #     if (type(refId) in (int, float)):
                #         refId = str(refId)
                #
                #     if (refId == None):
                #         refId = ""
                #     refId = validate_string_job_card(refId, table_column)
                #
                #
                # if (table_column == 'activity_name'):
                #     actId = item.get(json_column, None)
                #
                #     actId = str(actId)
                #
                #     if (actId == None):
                #         actId = ""
                #     actId = validate_string_job_card(actId, table_column)
                #
                # if (table_column == 'supervisor_number'):
                #     supNum = item.get(json_column, None)
                #
                #     if (type(supNum) in (int, float)):
                #         supNum = str(supNum)
                #
                #     if (supNum == None):
                #         supNum = ""
                #     supNum = validate_string_job_card(supNum, table_column)
                #
                # if (table_column == 'activity_creation_date'):
                #     createDate = ''+item.get(json_column, None)
                #     createDate = validate_string_job_card(createDate, table_column)
                #
                # if (table_column == 'activity_creation_time'):
                #     createTime = ''+item.get(json_column, None)
                #     createTime = validate_string_job_card(createTime, table_column)
                #
                # if (table_column == 'activity'):
                #     activityNum = item.get(json_column, None)
                #
                #     if (type(activityNum) in (int, float)):
                #         activityNum = str(activityNum)
                #
                #     if (activityNum == None):
                #         activityNum = ""
                #     activityNum = validate_string_job_card(activityNum, table_column)
                #     check = 1

                if (check == 1):
                    checkQuery = "SELECT COUNT(*) FROM alpl_activity_trans"
                    if (jobCardValue == 'NULL'):
                        whereQuery = " WHERE (job_card_id is NULL) AND"
                    else:
                        whereQuery = " WHERE (job_card_id = " + jobCardValue + ") AND"

                    # if (refId == 'NULL'):
                    #     whereQuery = whereQuery + " (alpl_reference_id is NULL) AND"
                    # else:
                    #     whereQuery = whereQuery + " (alpl_reference_id = " + refId + ") AND"
                    #
                    # if (actId == 'NULL'):
                    #     whereQuery = whereQuery + " (activity_name is NULL) AND"
                    # else:
                    #     whereQuery = whereQuery + " (activity_name = " + actId + ") AND"

                    if (spare_name == 'NULL'):
                        whereQuery = whereQuery + " (name_of_spart is NULL) AND"
                    else:
                        whereQuery = whereQuery + " (name_of_spart is NULL or name_of_spart = " + spare_name + ") AND"

                    if (spare_id == 'NULL'):
                        whereQuery = whereQuery + " (spare_part_id is NULL) AND"
                    else:
                        whereQuery = whereQuery + " (spare_part_id is NULL or spare_part_id = " + spare_id + ") AND"
                    if (supNum == 'NULL'):
                        whereQuery = whereQuery + " (supervisor_number is NULL) AND"
                    else:
                        whereQuery = whereQuery + " (supervisor_number="+str(0)+" or supervisor_number is NULL or supervisor_number = " + supNum + ") AND"
                    if (createDate == 'NULL'):
                        whereQuery = whereQuery + " (activity_creation_date is NULL) AND"
                    else:
                        whereQuery = whereQuery + " (activity_creation_date is NULL or activity_creation_date = " + createDate + ") AND"
                    if (createTime == 'NULL'):
                        whereQuery = whereQuery + " (activity_creation_time is NULL) AND"
                    else:
                        whereQuery = whereQuery + " (activity_creation_time is NULL or activity_creation_time = " + createTime + ") AND"
                    if (activityNum == 'NULL'):
                        whereQuery = whereQuery + " (activity is NULL)"
                    else:
                        whereQuery = whereQuery + " (activity is NULL or activity = " + activityNum + ")"
                    checkQuery = checkQuery + whereQuery
                    cursor.execute(checkQuery)
                    count = cursor.fetchone()[0]
                    check = 0
                    if (count == 0):
                        insertCheck = 1
                    else:
                        updateCheck = 1
                        # insertCheck=1

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
                        updateQuery = updateQuery + '=' + validate_string_job_card(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
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

                        elif (table_column == 'activity_creation_date_time'):
                            value = ""
                            value = str(value + creationDate + ' ' + creationTime)

                        elif (table_column == 'activity_creation_end_time'):
                            value = ""
                            value = str(value + endDate + ' ' + endTime)

                        elif (table_column == 'storage_location'):
                            if (value == ''):
                                value = ''
                            else:
                                value = dictLoc.get(int(value))

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string_job_card(value, table_column)

                    i = 1
                elif (insertCheck == 1):

                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)
                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        dataset = dataset + validate_string_job_card(value, table_column)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
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
                            if (value == ''):
                                value = ''
                            else:
                                value = dictLoc.get(int(value))

                        if (type(value) in (int, float)):
                            value = str(value)
                        elif (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string_job_card(value, table_column)

                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ',last_updated_time=' + currentTimestamp + ' ' + whereQuery
            totalQuery = query + dataset
            deleteQuery = 'delete from alpl_activity_trans where job_card_id=' + jobCardValue + ' and activity=' + activityNum + ' and (counter is null or counter=' + "'0'" + ')'
            if (flag == 1):
                q = updateQuery
                # logger.info(q)
                cursor.execute(updateQuery)
                # cursor.execute(totalQuery)
                # logger.info(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                if source == 'ACTIVITY_TRANS':
                    cursor.execute(deleteQuery)
                # logger.info(q)
                cursor.execute(totalQuery)
                # logger.info(totalQuery)
                cti = cti + 1

        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_activity_trans', source, item, q, str(e), filename, jcid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1

        else:
            p = p + 1
            con.commit()

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_activity_trans:" + str(p))
    logger.info("updated rows :" + str(ctu))
    logger.info("inserted rows :" + str(cti))
    logger.info("error rows :" + str(errors))
    logger.info("connection closed successfully")


def load_trip_maintenance_trans(json,source,con):
    logger.info("Data ingestion starting for alpl_trip_maintenance_trans")
    # json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    column_map={'alpl_trip_id':'TripID','vehicle_no':'VehRegNo','type':'MaintenanceType','generated_date':'GeneratedDate'}
    errors=0
    for i, item in enumerate(json):
        dataset=' values ('
        query='INSERT INTO alpl_trip_maintenance_trans ('
        i=0
        tripValue=0
        vehicle=''
        # tripId=0
        data={}
        flag=0
        q=None
        try:
            for table_column,json_column in column_map.items():
                if json_column=='MaintenanceType':
                    tripValue = int(validate_string_job_card(item.get('TripID', None), table_column))
                    vehicle= validate_string_job_card(item.get('VehRegNo', None), table_column)
                    generatedTime=validate_string_job_card(item.get('GeneratedDate', None), 'generated_date')
                    duplicateCheck = 'select * from alpl_trip_maintenance_trans where alpl_trip_id=' + str(tripValue) + ' and vehicle_no=' + vehicle + ' and type=' + validate_string_job_card(item.get(json_column, None), table_column)+' and generated_date='+generatedTime
                    cursor.execute(duplicateCheck)
                    duplicate = cursor.fetchall()
                    if item.get(json_column, None) == 'Maintenance' and not duplicate:
                        data['tripId']=int(tripValue)
                        auth_token='86ebc511-0e80-40d2-92fa-1d123b2c98bc'
                        hed = {'Authorization': 'Bearer ' + auth_token}
                        flag=1
                    elif item.get(json_column, None) == 'Accident' and not duplicate:
                        tripQuery="select max(trip_id) from alpl_trip_data where alpl_trip_id="+str(tripValue)
                        cursor.execute(tripQuery)
                        tripId = cursor.fetchall()
                        assetPresentQuery="select trip_id from alpl_asset_telemetry where trip_id="+str(tripId[0][0])+" and status="+"'ACCIDENT'"
                        cursor.execute(assetPresentQuery)
                        present=cursor.fetchall()
                        if not present:
                            assetQuery="insert into alpl_asset_telemetry(trip_id,asset_reg_number,message_timestamp,last_updated_time,status,created_time) values("+str(tripId[0][0])+", "+vehicle+", "+'extract(epoch from '+"'"+datetime.strptime(item.get('GeneratedDate', None), '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')+"'::timestamp at time zone 'Asia/Calcutta'"+')'+", "+'now()'+", "+"'ACCIDENT'"+", "+'now()'+")"
                            cursor.execute(assetQuery)

                    elif item.get(json_column, None) == 'BreakDown' and not duplicate:
                        tripQuery="select max(trip_id) from alpl_trip_data where alpl_trip_id="+str(tripValue)
                        cursor.execute(tripQuery)
                        tripId = cursor.fetchall()
                        assetPresentQuery = "select trip_id from alpl_asset_telemetry where trip_id=" + str(tripId[0][0]) + " and status=" + "'OBD'"
                        cursor.execute(assetPresentQuery)
                        present = cursor.fetchall()
                        if not present:
                            assetQuery = "insert into alpl_asset_telemetry(trip_id,asset_reg_number,message_timestamp,last_updated_time,status,created_time) values(" +str(tripId[0][0])+", "+ vehicle +", "+ 'extract(epoch from '+"'"+datetime.strptime(item.get('GeneratedDate', None), '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')+"'::timestamp at time zone 'Asia/Calcutta'"+')'+", " + 'now()'+ ", " + "'OBD'"+", "+'now()'+")"
                            cursor.execute(assetQuery)

                if i==0:
                    query=query+table_column
                    dataset=dataset+validate_string_job_card(item.get(json_column,None),table_column)
                else:
                    query=query+','+table_column
                    dataset=dataset+','+validate_string_job_card(item.get(json_column,None),table_column)
                i=1
            dataset=dataset+')'
            query=query+')'
            totalQuery=query+dataset
            q=totalQuery
            if not duplicate:
                cursor.execute(totalQuery)
        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_trip_maintenance_trans', source, item, q, str(e), filename,'0')
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p=p+1
            if(flag==1):
                requests.post("http://api.arunachala.cc/alplapp/jobcard/delayInJobCardWorkflow",params=data,headers=hed)
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_trip_maintenance_trans:"+str(p))
    logger.info("connection closed successfully")


def load_gatepass(json, source, con):
    logger.info("Data ingestion starting for alpl_purchase_info")
    cursor = con.cursor()
    ctu = 0
    cti = 0
    p = 0
    errors = 0
    q = None
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'
        
    cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_unit_master')
    result = cursor.fetchall()
    dictUnitc = dict(result)
    columMap = {'document_no': 'DOCNO',
                'document_date': 'DATE',
                'unit_id': 'PLANT',
                'document_type':'DOCTYPE',
                'number_of_items':'NOOFITEMS',
                'quantity':'QUANTITY',
                'inward_outward':'TYPE'
                }
    for i, item in enumerate(json):
        try:
            docno = str(item.get('DOCNO',None))
            dataset = 'values ('
            query = 'INSERT INTO alpl_purchase_info ('
            i = 0
            for table_column, json_column in columMap.items():
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

                    if (table_column == 'unit_id'):
                        if('ALPL' in value):
                            value='0'
                        else:
                            value = dictUnitc.get(int(value))

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""

                    dataset = dataset + ',' + validate_string_job_card(value, table_column)
                i = 1
            dataset=dataset+')'
            query=query+') '
            totalQuery=query+dataset
            q=totalQuery
            cursor.execute(totalQuery)
            cti = cti+1
        except Exception as e:
            logger.info(str(e) + str(i))
            con.rollback()
            try:
                Util.insert_error_record('alpl_purchase_info', source, item, q, str(e),filename,docno)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_activity_trans:" + str(p))
    logger.info("updated rows :" + str(ctu))
    logger.info("inserted rows :" + str(cti))
    logger.info("error rows :" + str(errors))
    logger.info("connection closed successfully")


def main(file_name):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # file = os.path.abspath('E:/json_files') + "/0714/model_test.json"
    try:
        # json_data = open(file).read()
        # json_obj = json.loads(json_data)
        global filename
        filename = file_name
        json_obj = Util.getJsonFile(Util.maintenance_bkt, file_name)
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
        func=None
        if source == 'JOBCARD_TRANS':
            func = load_job_card_trans
        elif source == 'ACTIVITY_TRANS':
            func = load_activity_trans
        elif source == 'ACTIVITY_TRANS_WORKF':
            func = load_activity_trans
        elif source == 'TRIP_MAINTENANCE_TRANS':
            func= load_trip_maintenance_trans
        elif source=='GATEPASS':
            func = load_gatepass
        if func!=None:
            MultiProc.runDataIngestion(jsonData, func, source)
            Util.move_blob(Util.maintenance_bkt, file_name, dest_bucket, file_name)
            Util.call_error_data_url(filename, sourcetype)
        else:
            logger.info("No CF implemented for source: "+source)
        # func(jsonData,source)
    except Exception as ex:
        logger.info("Halting json data load processing due to following exception:" + str(ex))
        Util.move_blob(Util.maintenance_bkt, file_name, Util.error_bucket, file_name)
        Util.send_alert(file_name, str(ex))