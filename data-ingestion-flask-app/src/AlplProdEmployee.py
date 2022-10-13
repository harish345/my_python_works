from datetime import datetime
import calendar
import Util
import MultiProc
from LoggingUtil import logger

currentTime = datetime.now();
currentTimestamp = str(currentTime)
currentTimestamp = "'" + currentTimestamp[:currentTimestamp.find('.')] + "'"
currentTimestamp='now()'

units = {}
# empIdList = []
shifts = {}
depts = {}
empPair = {}
plants={}

filename=''

def variableInit(cursor):
    cursor.execute('SELECT alpl_unit_code,unit_id from alpl_unit_master')
    rows = cursor.fetchall()
    for row in rows:
        units[str(row[0])] = int(row[1])

    cursor.execute('SELECT alpl_plant_code,plant_id from alpl_plant_master')
    rows = cursor.fetchall()
    for row in rows:
        plants[str(row[0])] = int(row[1])

    cursor.execute('SELECT alpl_dept_id,dept_id from alpl_department_master')
    rows = cursor.fetchall()
    for row in rows:
        depts[str(row[0])] = int(row[1])
    # logger.info(depts)
    # cursor.execute('SELECT alpl_emp_id from alpl_employee_master')
    # result=cursor.fetchall()
    # empIdList=[i[0] for i in result]
    cursor.execute('SELECT upper(concat(shift,br_shift)),shift_id from alpl_shift_master')
    rows = cursor.fetchall()
    for row in rows:
        shifts[row[0]] = int(row[1])
    cursor.execute('SELECT alpl_emp_id,emp_id from alpl_employee_master')
    rows = cursor.fetchall()
    for row in rows:
        empPair[str(row[0])] = int(row[1])


def load_alpl_employee_attendance(json, source, con):
    logger.info("Data ingestion starting for alpl_employee_attendance")
    # json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    variableInit(cursor)
    p = 0
    errors = 0
    ins = 0
    upd = 0
    src = 'logistics'
    emp = '0'
    dateUp = '0'
    column_map = {'comoff_leave': 'COM_OFF',
                  'date': 'DATE',
                  'early_swipe_out': 'EARLY_PUNCH',
                  'late_swipe_in': 'LATE_PUNCH',
                  'no_swipe': '',
                  'on_duty': '',
                  'on_leave': '',
                  'dept_id': 'DEPT',
                  'role_id': 'JOBKEY',
                  'unit_id': '',
                  'is_present': '',
                  'zb_shift': 'ZBSHIFT',
                  'shift_id': '',
                  'emp_id': '',
                  'alpl_emp_id': 'EMPCODE',
                  'is_absent': '',
                  'punch_in': 'PUNCH_IN',
                  'punch_out': 'PUNCH_OUT',
                  'plant_area': 'PLANT_AREA'
                  }
    for i, item in enumerate(json):
        value = None
        brShift = None
        shift = None
        q = None
        storeVal = None
        empcode=item.get('EMPCODE',None)
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_employee_attendance ('
            updateQuery = 'UPDATE alpl_employee_attendance SET '
            i = 0
            for table_column, json_column in column_map.items():
                if i == 0:
                    if json_column == '':
                        query = query + table_column
                        storeVal = validate_func(item, table_column)
                        # dataset=dataset+validate_func(item,table_column)
                        dataset = dataset + storeVal
                        updateQuery = updateQuery + table_column
                        updateQuery = updateQuery + '=' + storeVal
                    else:
                        query = query + table_column
                        storeVal = validate_string(item.get(json_column, None), table_column)
                        # dataset=dataset+validate_string(item.get(json_column,None),table_column)
                        dataset = dataset + storeVal
                        updateQuery = updateQuery + table_column
                        updateQuery = updateQuery + '=' + storeVal
                        if table_column == 'alpl_emp_id':
                            emp = validate_string(item.get(json_column, None), table_column)
                        elif (table_column == 'date'):
                            dateUp = validate_string(item.get(json_column, None), table_column)
                else:
                    if json_column == '':
                        query = query + ',' + table_column

                        if (table_column == 'shift_id'):
                            shift = validate_func(item, table_column)
                            value = getShiftId(shift, brShift)
                            value = validate_string(value, table_column)
                        else:
                            value = validate_func(item, table_column)
                        dataset = dataset + ',' + str(value)
                        updateQuery = updateQuery + ',' + table_column
                        updateQuery = updateQuery + '=' + str(value)

                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if (table_column == 'zb_shift'):
                            brShift = value
                        storeVal = validate_string(value, table_column)
                        # dataset=dataset+','+validate_string(value,table_column)
                        dataset = dataset + ',' + storeVal
                        updateQuery = updateQuery + ',' + table_column
                        updateQuery = updateQuery + '=' + storeVal
                        if table_column == 'alpl_emp_id':
                            emp = validate_string(item.get(json_column, None), table_column)
                        elif (table_column == 'date'):
                            dateUp = validate_string(item.get(json_column, None), table_column)

                i = 1
            dataset = dataset + ')'
            updateQuery = updateQuery + ',last_updated_time=' + currentTimestamp + ' where alpl_emp_id=' + emp + ' and date=' + dateUp
            selectQuery = 'select * from alpl_employee_attendance' + ' where alpl_emp_id=' + emp + ' and date=' + dateUp
            query = query + ')'
            totalQuery = query + dataset
            # logger.info(totalQuery)
            q = totalQuery
            # cursor.execute(totalQuery)
            # logger.info(updateQuery)
            # logger.info(cursor.execute(selectQuery))
            cursor.execute(selectQuery)
            result = cursor.fetchall()
            if (result):
                cursor.execute(updateQuery)
                upd = upd + 1
            else:
                cursor.execute(totalQuery)
                ins = ins + 1
        except Exception as e:
            logger.info(str(e))
            con.rollback()

            try:
                Util.insert_error_record('alpl_employee_attendance', source, item, q, str(e),filename,empcode)
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
    logger.info("Data ingestion completed. No of rows processed for alpl_employee_attendance:" + str(p))
    logger.info("inserted rows " + str(ins))
    logger.info("updated rows " + str(upd))
    logger.info('errors :' + str(errors))
    logger.info("connection closed successfully")


def load_alpl_employee_master(json, source, con):
    logger.info("Data ingestion starting for alpl_employee_master")
    # json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    variableInit(cursor)
    cursor.execute('SELECT alpl_emp_id from alpl_employee_master')
    result = cursor.fetchall()
    empIdList = [i[0] for i in result]
    p = 0
    icnt = 0
    ucnt = 0
    errors = 0
    unit_column_map = {'alpl_emp_id': 'ALPLREFID',
                  'emp_name': 'EMPNAME',
                  'email': 'EMAIL_ID',
                  'manager': 'MANAGER',
                  'phone_number': 'PHONE_NUMBER',
                  'photo_name': 'PHOTO',
                  'dept_id': 'DEPARTMENT',
                  'role_id': 'JOBKEY',
                  'creation_time': 'CREATION_DATE',
                  'unit_id': 'UNIT',
                  'employment_date': 'EMPLOY_DATE',
                  'role_name': 'JOBKEY_DESC',
                  'status': 'STATUS'}

    godown_column_map = {'alpl_emp_id': 'ALPLREFID',
                  'emp_name': 'EMPNAME',
                  # 'email': 'EMAIL_ID',
                  'manager': 'MANAGER',
                  'phone_number': 'PHONE_NUMBER',
                  'photo_name': 'PHOTO',
                  'dept_id': 'DEPARTMENT',
                  'role_id': 'JOBKEY',
                  'creation_time': 'CREATION_DATE',
                  'plant_id': 'PLANT_AREA',
                  'plant_area': 'PLANT_AREA',
                  'employment_date': 'EMPLOY_DATE',
                  'role_name': 'JOBKEY_DESC',
                  'status': 'STATUS'
                  }

    if (source == 'HR_MASTER'):
        column_map = unit_column_map
    else:
        column_map = godown_column_map
    for i, item in enumerate(json):
        dataset = 'values ('
        insertQuery = 'INSERT INTO alpl_employee_master ('
        updateQuery = 'UPDATE alpl_employee_master SET '
        i = 0
        flag = 0
        src = 'logistics'
        q = None
        value = '0'
        empid = item.get('ALPLREFID',None)
        try:
            for table_column, json_column in column_map.items():
                val = item.get(json_column, None)
                if (flag == 1) or (table_column == 'alpl_emp_id' and (
                        int(validate_string_attendence(item.get(json_column, None), table_column)) in empIdList)):
                    if flag == 0:
                        value = validate_string_attendence(item.get(json_column, None), table_column)
                    flag = 1
                    if i == 0:
                        updateQuery = updateQuery + table_column
                        updateQuery = updateQuery + '=' + validate_string_attendence(item.get(json_column, None),
                                                                                     table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        updateQuery = updateQuery + '=' + validate_string_attendence(item.get(json_column, None),
                                                                                     table_column)
                        if(table_column=='status' and val=='NON-ACTIVE'):
                            update_user='update alpl_user set enabled=False where alpl_emp_id='+str(empid)
                            cursor.execute(update_user)
                    i = 1
                else:
                    if i == 0:
                        insertQuery = insertQuery + table_column
                        dataset = dataset + validate_string_attendence(item.get(json_column, None), table_column)
                    else:
                        insertQuery = insertQuery + ',' + table_column
                        dataset = dataset + ',' + validate_string_attendence(item.get(json_column, None), table_column)
                        if(table_column=='status' and val=='NON-ACTIVE'):
                            update_user='update alpl_user set enabled=False where alpl_emp_id='+str(empid)
                            cursor.execute(update_user)
                    i = 1
            dataset = dataset + ')'
            insertQuery = insertQuery + ')'
            updateQuery = updateQuery + ',last_updated_time=' + currentTimestamp + ' where alpl_emp_id=' + value
            totalQuery = insertQuery + dataset
            if flag:
                q = updateQuery
                ucnt = ucnt + 1
                cursor.execute(updateQuery)
                # logger.info(updateQuery)
            else:
                q = totalQuery
                icnt = icnt + 1
                cursor.execute(totalQuery)
                # logger.info(totalQuery)

        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_employee_master', source, item, q, str(e),filename,empid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_employee_master:" + str(p))
    logger.info("errors:" + str(errors))
    logger.info("inserted :" + str(icnt))
    logger.info("updated :" + str(ucnt))
    logger.info("connection closed successfully")

def load_hr_leaves(json,source,con):
    logger.info("leaves,onduty updation starting for alpl_employee_attendance")
    cursor=con.cursor()
    p = 0
    upd = 0
    errors = 0
    on_leave= ''
    on_duty= ''
    leave=["SPL","SL","CL","MCO","FCO","ACO"]
    onduty=["MOD","FOD","AOD","SOD"]
    isabsent=["LOP"]
    column_map = {'alpl_emp_id': 'PERNR',
                  'date': 'ZDATE'
                  }
    for i, item in enumerate(json):
        value = None
        empid = item.get('PERNR',None)
        try:
            updateQuery = 'UPDATE alpl_prod.alpl_employee_attendance SET '
            i=0
            for table_column,json_column in column_map.items():
                if i == 0:
                    updateQuery = updateQuery + table_column
                    updateQuery = updateQuery + '=' + validate_string(item.get(json_column, None),
                                                                                 table_column)
                else:
                    updateQuery = updateQuery + ',' + table_column
                    updateQuery = updateQuery + '=' + validate_string(item.get(json_column, None),
                                                                                 table_column)

                i = 1
            if(str(item.get('ZTYPE', None)) in leave):
                updateQuery=updateQuery + ',is_present=' + "'false'" + ',is_absent=' + "'false'" + ',on_duty=' + "'false'" + ',on_leave=' + "'true'"
            if (str(item.get('ZTYPE', None)) in onduty):
                updateQuery = updateQuery + ',is_present=' + "'false'" + ',is_absent=' + "'false'" + ',on_duty=' + "'true'" + ',on_leave=' + "'false'"
            if (str(item.get('ZTYPE', None)) in isabsent):
                updateQuery = updateQuery + ',is_present=' + "'false'" + ',is_absent=' + "'true'" + ',on_duty=' + "'false'" + ',on_leave=' + "'false'"
            updateQuery=updateQuery+',last_updated_time='+currentTimestamp+' where alpl_emp_id='+validate_string(item.get('PERNR', None),'alpl_emp_id')+' and date='+validate_string(item.get('ZDATE', None),'date')
            selectQuery='select * from alpl_employee_attendance'+' where alpl_emp_id='+validate_string(item.get('PERNR', None),'alpl_emp_id')+' and date='+validate_string(item.get('ZDATE', None),'date')
            cursor.execute(selectQuery)
            result = cursor.fetchall()
            if (result):
                cursor.execute(updateQuery)
                # logger.info(updateQuery)
                upd = upd + 1
            else:
                logger.info('this record is not present in attendance ',updateQuery)
        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('hr_leaves', source, item, updateQuery, str(e),filename,empid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_employee_master:" + str(p))
    logger.info("errors:" + str(errors))
    # logger.info("inserted :" + str(icnt))
    logger.info("updated :" + str(upd))
    logger.info("connection closed successfully")

def validate_string(val, val1):
    val=str(val)
    if val1 == 'dept_id':
        return str(depts.get(str((int(val)))))
    elif (type(val) in (int, float)):
        return str(val)
    elif val.replace('.', '', 1).isdigit():
        return val

    if ((val.find("'", 0, len(val)) >= 0)):
        val = val.replace('\'', '\\\'')
        return 'E' + "'" + val + "'"

    elif val1 == 'employment_date' or val1 == 'creation_time':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'
    elif val != None and val != "":
        return "'" + val + "'"
    else:
        return 'NULL'


def validate_string_attendence(val, val1):
    if val1 == 'unit_id':
        if val == 'ALPL':
            val = '1'
        return str(units[val])
    elif val1=='plant_id':
        return str(plants[val])
    elif val1 == 'dept_id':
        return str(depts.get(str((int(val)))))
    elif val1 == 'manager':
        if empPair.get(str((int(val)))) == None:
            return 'NULL'
        else:
            return str(empPair.get(str((int(val)))))
    else:
        return validate_string(val, val1)


def getUnit(val, val1):
    if val == 'ALPL':
        val = '1'
    return str(units[val])


#
# def getManager(val,val1):
#     # cursor = con.cursor()
#     if val != None and val!="":
#         q="SELECT emp_id from alpl_employee_master where alpl_emp_id="+str(val)
#         eid=cursor.execute(q)
#         empId=cursor.fetchall()
#         if not empId:
#             logger.info(val)
#             return 'NULL'
#         else:
#             return str(empId[0][0])
#     else:
#         return 'NULL'

def validate_func(item, val1):
    # cursor = con.cursor()
    if val1 == 'no_swipe':
        if item.get('PUNCH_IN', None) == '00:00:00' and item.get('PUNCH_OUT', None) == '00:00:00':
            return validate_string('true', val1)
        else:
            return validate_string('false', val1)
    elif val1 == 'emp_id':
        # q="SELECT emp_id from alpl_employee_master where alpl_emp_id="+validate_string(item.get('EMPCODE',None),val1)
        # eid=cursor.execute(q)
        # empId=cursor.fetchall()
        return str(empPair.get(str((int(item.get('EMPCODE', None))))))
    elif val1 == 'unit_id':
        val = item.get('UNIT', None)
        return getUnit(val, val1)
    elif val1 == 'shift_id':
        if item.get('ABS_SHIFT', None) != '':
            v = item.get('ABS_SHIFT', None)
            if (v == 'WEEKLY OFF'):
                v = 'WO'
            elif (v == 'PUBLIC HOL'):
                v = 'PH'
            return str(v)
        elif item.get('PRESENT_SHIFT', None) != '':
            v = item.get('PRESENT_SHIFT', None)
            if (v == 'WEEKLY OFF'):
                v = 'WO'
            elif (v == 'PUBLIC HOL'):
                v = 'PH'
            return str(v)
        # else:
        #     return str('G')
    elif val1 == 'on_leave':
        if item.get('LEAVE', None) != '' or item.get('COM_OFF', None) != '':
            return validate_string('true', val1)
        else:
            return validate_string('false', val1)
    elif val1 == 'on_duty':
        if item.get('ONDUTY', None) != '':
            return validate_string('true', val1)
        else:
            return validate_string('false', val1)
    elif val1 == 'is_present':
        if item.get('LEAVE', None) == '' and item.get('COM_OFF', None) == '' and item.get('ONDUTY',
                                                                                          None) == '' and item.get(
                'ABS_SHIFT', None) == '' and item.get('PRESENT_SHIFT', None) != '' and item.get('PRESENT_SHIFT',
                                                                                                None) != 'WEEKLY OFF' and item.get(
                'PRESENT_SHIFT', None) != 'PUBLIC HOL':
            return validate_string('true', val1)
        else:
            return validate_string('false', val1)
    else:
        if item.get('LEAVE', None) == '' and item.get('COM_OFF', None) == '' and item.get('ONDUTY',
                                                                                          None) == '' and item.get(
                'ABS_SHIFT', None) != '' and item.get('PRESENT_SHIFT', None) != 'WEEKLY OFF' and item.get(
                'PRESENT_SHIFT', None) != 'PUBLIC HOL':
            return validate_string('true', val1)
        else:
            return validate_string('false', val1)


def getShiftId(shift, shiftCode):
    shiftCombine = str(shift) + str(shiftCode)
    shiftCombine = shiftCombine.upper()
    return shifts[str(shiftCombine)]


def load_shift_roster(json, source, con):
    logger.info("Data ingestion starting for alpl_shift_roster")
    # json_obj = getJsonFile(json,source)
    partsLists = []
    cursor = con.cursor()
    variableInit(cursor)
    # cursor.execute('select alpl_part_id from  alpl_parts_master ')
    # res=cursor.fetchall()
    # partsLists = [i[0] for i in res]
    ctu = 0
    cti = 0
    p = 0
    errors = 0
    q = None
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'

    columMap = {'alpl_emp_id': 'PERNR',
                'emp_name': 'ZENAME',
                'unit_id':'UNIT',
               'plant_area':'PLANT_AREA'
                }

    for i, item in enumerate(json):
        try:
            empid = item.get('PERNR',None)
            dataset = 'values ('
            query = 'INSERT INTO alpl_shift_roster ('
            i = 0
            month = item.get('ZMONTH', None)
            year = item.get('ZYEAR', None)
            shiftCode = item.get('ZBSHIFT', None)
            days = calendar.monthrange(int(year), int(month))[1]
            for table_column, json_column in columMap.items():
                if i == 0:
                    query = query + table_column
                    value = item.get(json_column, None)
                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""
                    dataset = dataset + validate_string_attendence(value, table_column)
                else:
                    query = query + ',' + table_column
                    value = item.get(json_column, None)

                    # if (table_column == 'unit_id'):
                    #     value = getUnit(value, table_column)

                    if (type(value) in (int, float)):
                        value = str(value)
                    if (value == None):
                        value = ""

                    dataset = dataset + ',' + validate_string_attendence(value, table_column)
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
                shift = item.get(dayShift, None)
                value = getShiftId(shift, shiftCode)

                if (type(value) in (int, float)):
                    value = str(value)
                if (value == None):
                    value = ""

                startDate = str(year) + '-' + str(month) + '-' + str(dayNum)
                startDate = validate_string_attendence(startDate, 'startDate')
                endDate = startDate
                shiftId = validate_string_attendence(value, 'shiftId')

                q = q + ',' + 'start_date,end_date,shift_id)'
                d = d + ',' + str(startDate) + ',' + str(endDate) + ',' + str(shiftId)
                d = d + ')'
                totalQuery = q + d
                # logger.info(totalQuery)
                cursor.execute(totalQuery)
        except Exception as e:
            logger.info(str(e) + str(i))
            logger.info(query)
            logger.info(dataset)
            con.rollback()
            try:
                Util.insert_error_record('alpl_shift_roster', source, item, q, str(e),filename,empid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()

    logger.info("Data ingestion completed. No of rows processed for alpl_shift_roster:" + str(p))
    logger.info("connection closed successfully")
    logger.info("inserted rows:" + str(cti))
    logger.info("updated rows:" + str(ctu))
    logger.info("errors :" + str(errors))

def salary(json, source,con):
    logger.info("Data ingestion starting for alpl_hr_salary_maintenence")
    # json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    errors = 0
    src = 'logistics'
    variableInit(cursor)
    # if ("MINING" in json_obj[source]):
    #     src = 'mining'
    column_map = {'emp_id': 'ALPLREFID',
                  'emp_name': 'EMPNAME',
                  'role_id': 'ROLE',
                  'role_name': 'ROLE_NAME',
                  'unit_id': 'UNIT',
                  'unit_name': 'UNIT_NAME',
                  'dept_no': 'DEPARTMENT',
                  'dept_name': 'DEPT_NAME',
                  'salary': 'SALARY',
                  'sub_group': 'SUBGROUP'
                  }
    uct = 0
    ict = 0
    empIdQuery = 'SELECT emp_id FROM alpl_hr_salary_maintenance'
    cursor.execute(empIdQuery)
    result = cursor.fetchall()
    empIdList = [i[0] for i in result]
    for i, item in enumerate(json):

        dataset = 'values ('
        query = 'INSERT INTO alpl_hr_salary_maintenance ('
        updateQuery = 'UPDATE alpl_hr_salary_maintenance SET '
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
                            value = getUnit(value,'unit_id')

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
                            value = getUnit(value,'unit_id')

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
                cursor.execute(updateQuery)
                uct = uct + 1
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                ict = ict + 1
        except Exception as e:
            logger.info(str(e) + str(i))
            logger.info(q)
            logger.info(dataset)
            con.rollback()
            try:
                Util.insert_error_record('alpl_hr_salary', source, item, q, str(e), filename, valueID)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_hr_salary_maintenence:"+ str(p))
    logger.info("inserted :"+ str(ict))
    logger.info("updated :"+ str(uct))
    logger.info("errors :"+ str(errors))
    logger.info("connection closed successfully")


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
        json_obj = Util.getJsonFile(Util.employee_bkt, file_name)
        logger.info(f"Processing file: " + file_name)
        # logger.info("Processing file: ")
        global filename
        filename = file_name
        data = str(json_obj)
        data = data[2:8]
        sourcetype = 'PORTAL'
        if (data == 'Source'):
            dataSource = 'data'
            dest_bucket = Util.portal_processed_bucket
        elif (data == 'SOURCE'):
            dataSource = 'DATA'
            dest_bucket = Util.sap_processed_bucket
            sourcetype = 'SAP'
        jsonData = json_obj[dataSource]
        source = json_obj[data]

        if source == 'HR_MASTER':
            func = load_alpl_employee_master
        if source == 'GODOWN_CP_MASTER':
            func = load_alpl_employee_master
        if source == 'HR_PUNCHING_DATA':
            func = load_alpl_employee_attendance
        if source == 'SHIFT_SCHEDULE':
            func = load_shift_roster
        if source == 'HR_LEAVES_DATA':
            func = load_hr_leaves
        if source == 'HR_SALARY_MAINTENANC':
            func = salary
        # runDataIngestion(jsonData, func, source)
        # func(jsonData,source)
        MultiProc.runDataIngestion(jsonData, func, source)
        Util.move_blob(Util.employee_bkt, file_name, dest_bucket, file_name)
        #Util.call_error_data_url(filename, sourcetype)
    except Exception as ex:
        logger.info("Halting json data load processing due to following exception:" + str(ex))
        Util.move_blob(Util.employee_bkt, file_name, Util.error_bucket, file_name)
        Util.send_alert(file_name, str(ex))