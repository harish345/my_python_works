import psycopg2;
import os, json;
from datetime import datetime;
import multiprocessing as mp;
import time;
from multiprocessing import Process;

# read JSON file which is in the next parent folder
file = os.path.abspath('D:/json_files') + "/attend.json"
json_data = open(file).read()
json_obj = json.loads(json_data)

units={}
empIdList = []
shifts={}
con = psycopg2.connect(user="postgres",
                       password="admin",
                       host="34.93.62.36",
                       port="5432",
                       database="alpl_test")

cursor = con.cursor()
cursor.execute('SELECT alpl_unit_code,unit_id from alpl_uat.alpl_unit_master')
rows=cursor.fetchall()
for row in rows:
    units[str(row[0])] = int(row[1])
cursor.execute('SELECT alpl_emp_id from alpl_uat.alpl_employee_master')
result=cursor.fetchall()
empIdList=[i[0] for i in result]
cursor.execute('SELECT upper(concat(shift,br_shift)),shift_id from alpl_uat.alpl_shift_master')
rows=cursor.fetchall()
for row in rows:
    shifts[row[0]] = int(row[1])

def validate_string(val,val1):
    if (type(val) in (int, float)):
        return str(val)
    elif val.replace('.','',1).isdigit():
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
    elif val != None and val!="":
        return "'"+val+"'"
    else:
        return 'NULL'

def validate_string_attendence(val,val1):
    if val1=='unit_id':
        return getUnit(val,val1)
    elif val1=='manager':
        return getManager(val,val1)
    else:
        return validate_string(val,val1)

def getShiftId(shift,shiftCode):
    shiftCombine = str(shift) + str(shiftCode)
    shiftCombine = shiftCombine.upper()
    return shifts[str(shiftCombine)]


def getUnit(val,val1):
    if val=='3021' or val=='3041':
        val='1007'
    elif val=='ALPL':
        val='1'
    elif val=='3030' or val=='3047' or val=='3048' or val=='2007':
        val='1004'
    elif val=='1251':
        val='1005'
    elif val=='2039':
        val='1009'
    elif val=='2030':
        val='1008'
    return str(units[val])

def getManager(val,val1):
    cursor = con.cursor()
    if val != None and val!="":
        q="SELECT emp_id from alpl_employee_master where alpl_emp_id="+str(val)
        eid=cursor.execute(q)
        empId=cursor.fetchall()
        if not empId:
            print(val)
            return 'NULL'
        else:
            return str(empId[0][0])
    else:
        return 'NULL'

def validate_func(item,val1):
    cursor = con.cursor()
    if val1=='no_swipe':
        if item.get('PUNCH_IN',None)=='00:00:00' and item.get('PUNCH_OUT',None)=='00:00:00':
            return validate_string('false',val1)
        else:
            return validate_string('true',val1)
    elif val1=='emp_id':
        q="SELECT emp_id from alpl_employee_master where alpl_emp_id="+validate_string(item.get('EMPCODE',None),val1)
        eid=cursor.execute(q)
        empId=cursor.fetchall()
        return str(empId[0][0])
    elif val1=='unit_id':
        val=item.get('UNIT',None)
        return getUnit(val,val1)
    elif val1=='shift_id':
        if item.get('PRESENT_SHIFT',None)!='':
            return str(item.get('PRESENT_SHIFT',None))
        elif item.get('ABS_SHIFT',None)!='':
            return str(item.get('ABS_SHIFT',None))
        else:
            return str('G')
    elif val1=='on_leave':
        if item.get('LEAVE',None)!='':
            return validate_string('true',val1)
        else:
            return validate_string('false',val1)
    elif val1=='on_duty':
        if item.get('ONDUTY',None)!='':
            return validate_string('true',val1)
        else:
            return validate_string('false',val1)
    elif val1=='is_present':
        if item.get('LEAVE',None)=='' and item.get('ONDUTY',None)=='' and item.get('PRESENT_SHIFT',None)!='':
            return validate_string('true',val1)
        else:
            return validate_string('false',val1)
    else:
        if item.get('LEAVE',None)=='' and item.get('ONDUTY',None)=='' and item.get('ABS_SHIFT',None)!='':
            return validate_string('true',val1)
        else:
            return validate_string('false',val1)

def load_alpl_employee_master(json,source):
    print("Data ingestion starting for alpl_employee_master")
    #json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    icnt = 0
    ucnt = 0
    column_map = {'alpl_emp_id':'ALPLREFID',
                  'emp_name':'EMPNAME',
                  'email':'EMAIL_ID',
                  'manager':'MANAGER',
                  'phone_number':'PHONE_NUMBER',
                  'photo_name':'PHOTO',
                  'dept_id':'DEPARTMENT',
                  'role_id':'ROLE',
                  'creation_time':'CREATION_DATE',
                  'unit_id':'UNIT',
                  'employment_date':'EMPLOY_DATE'}

    for i, item in enumerate(json):
        dataset='values ('
        insertQuery='INSERT INTO alpl_uat.alpl_employee_master ('
        updateQuery='UPDATE alpl_uat.alpl_employee_master SET '
        i=0
        flag=0
        value='0'
        try:
            for table_column,json_column in column_map.items():
                if (flag==1) or (table_column=='alpl_emp_id' and (int(validate_string_attendence(item.get(json_column,None),table_column)) in empIdList)):
                    if flag==0:
                        value=validate_string_attendence(item.get(json_column,None),table_column)
                    flag=1
                    if i==0:
                        updateQuery=updateQuery+table_column
                        updateQuery=updateQuery+'='+validate_string_attendence(item.get(json_column,None),table_column)
                    else:
                        updateQuery=updateQuery+','+table_column
                        updateQuery=updateQuery+'='+validate_string_attendence(item.get(json_column,None),table_column)

                    i=1
                else:
                    if i==0:
                        insertQuery=insertQuery+table_column
                        dataset=dataset+validate_string_attendence(item.get(json_column,None),table_column)
                    else:
                        insertQuery=insertQuery+','+table_column
                        dataset=dataset+','+validate_string_attendence(item.get(json_column,None),table_column)

                    i=1
            dataset=dataset+')'
            insertQuery=insertQuery+')'
            updateQuery=updateQuery+' where alpl_emp_id='+value
            totalQuery=insertQuery+dataset
            if flag:
                ucnt = ucnt + 1
                cursor.execute(updateQuery)
            else:
                icnt = icnt + 1
                cursor.execute(totalQuery)

        except Exception as e:
            #print(e)
            con.rollback()
        else:
            con.commit()
            p=p+1
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_employee_master:", p)
    print("inserted :",icnt)
    print("updated :",ucnt)
    print("connection closed successfully")



def load_alpl_employee_attendance(json,source):
    print("Data ingestion starting for alpl_employee_attendance")
    #json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = con.cursor()
    p = 0
    errors = 0
    src = 'logistics'
    column_map = {'comoff_leave':'COM_OFF',
                  'date':'DATE',
                  'early_swipe_out':'EARLY_PUNCH',
                  'late_swipe_in':'LATE_PUNCH',
                   'no_swipe':'',
                  'on_duty':'',
                  'on_leave':'',
                  'dept_id':'DEPT',
                  'role_id':'ROLE',
                  'unit_id':'',
                  'is_present':'',
                  'zb_shift': 'ZBSHIFT',
                  'shift_id':'',
                  'emp_id':'',
                  'alpl_emp_id':'EMPCODE',
                  'is_absent':''
                  }
    for i, item in enumerate(json):
        value = None
        brShift =None
        shift = None
        q = None
        try:
            dataset='values ('
            query='INSERT INTO alpl_uat.alpl_employee_attendance ('
            i=0
            for table_column,json_column in column_map.items():
                if i==0:
                    if json_column=='':
                        query=query+table_column
                        dataset=dataset+validate_func(item,table_column)
                    else:
                        query=query+table_column
                        dataset=dataset+validate_string(item.get(json_column,None),table_column)
                else:
                    if json_column=='':
                        query=query+','+table_column

                        if(table_column == 'shift_id'):
                            shift = validate_func(item,table_column)
                            value = getShiftId(shift,brShift)
                            value = validate_string(value, table_column)
                        else:
                            value = validate_func(item, table_column)
                        dataset=dataset+','+str(value)
                    else:
                        query=query+','+table_column
                        value = item.get(json_column,None)
                        if(table_column == 'zb_shift'):
                            brShift = value
                        dataset=dataset+','+validate_string(value,table_column)

                i=1
            dataset=dataset+')'
            query=query+')'
            totalQuery=query+dataset
            #print(totalQuery)
            q = totalQuery
            cursor.execute(totalQuery)
        except Exception as e:
            #print(e)
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
                errorQuery = errorQuery + validate_string('alpl_employee_attendance', 'tab') + ',' + str(validate_string(src, 'tab')) + ',' + str(validate_string(q, 'tab')) + ','
                errorQuery = errorQuery + str(validate_string(str(e), 'tab')) + ')'
                cursor.execute(errorQuery)
            except Exception as exp:
                print(exp)
            else:
                con.commit()
                errors = errors + 1
        else:
            # print("data inserted successfully")
            con.commit()
            p=p+1
    con.close()
    print("Data ingestion completed. No of rows processed for alpl_employee_attendance:", p)
    print('errors :',errors)
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

