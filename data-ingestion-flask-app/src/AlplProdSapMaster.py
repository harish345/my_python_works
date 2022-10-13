from datetime import datetime
import Util
import MultiProc
from LoggingUtil import logger

# conn = Util.conn
# logger.info(conn)

units = {}
depts = {}

filename=''


def get_units_depts(cursor):
    cursor.execute('SELECT alpl_unit_code,unit_id from alpl_unit_master')
    rows = cursor.fetchall()
    for row in rows:
        units[str(row[0])] = int(row[1])

    cursor.execute('SELECT alpl_dept_id,dept_id from alpl_department_master')
    rows = cursor.fetchall()
    for row in rows:
        depts[str(row[0])] = int(row[1])


def get_units(cursor):
    cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_unit_master')
    result_u = cursor.fetchall()
    dictUnitcc = dict(result_u)
    return dictUnitcc


def getUnitId(unit, dictUnitcc):
    if (unit == 'ALPL'):
        return 6
    return dictUnitcc.get(int(unit))


def validate_string_unit_dept(val, val1):
    if val1 == 'unit_id':
        if val == 'ALPL':
            val = '1'
        return str(units[val])
    elif val1 == 'dept_id':
        return str(depts.get(str((int(val)))))


def validate_string(val, val1):
    val=str(val)
    if val!=None and val.isnumeric():
        return val
    elif val!=None and val.replace('.', '', 1).isdigit():
        if val != None and val != "":
            return val
        else:
            return 'NULL'
    elif val1 == 'created_date_time' or val1 == 'stock_date_time':
        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y/%m/%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'

    elif (val1 == 'asset_reg_date' or val1 == 'asset_valid_till' or val1 == 'created_time' or val1 == 'asset_modf_date'
     or val1 == 'asset_dec_date' or val1 == 'expiry_date' or val1 == 'purchase_date' or val1 == 'decomiss_date' 
     or val1 == 'fitment_date' or val1 == 'date' or val1 == 'grn_date' or val1 == 'indent_issued_date'
     or val1 == 'tyre_purchase_date' or val1 == 'tyre_war_valid_date' or val1 == 'tyre_scrap_date' or val1 == 'rdate' or val1 == 'valid_to' or val1 == 'valid_from' or val1 == 'startDate' or val1=='removal_date' or val1=='warrenty_end_date' or val1=='warrenty_start_date'):

        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d').strftime('%Y-%m-%d') + "'"
        else:
            return 'NULL'

    elif val1 == 'last_updated_time':

        if (val == '0000-00-00'):
            return 'NULL'
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %I:%M:%S %p') + "'"
        else:
            return 'NULL'


    elif val != None and val != "":
        if "'" in val:
            return "'" + val.replace("'", "''") + "'"
        return "'" + val + "'"
    else:
        return 'NULL'


def load_unit_dept_mapping(json, source, conn):
    logger.info("Data ingestion starting for alpl_unit_dept_mapping")
    # json_obj = getJsonFile(bucketname, filename_with_path)
    cursor = conn.cursor()
    get_units_depts(cursor)
    p = 0
    icnt = 0
    ucnt = 0
    errors = 0
    column_map = {'unit_id': 'UNIT',
                  'dept_id': 'DEPARTMENT'
                  }

    for i, item in enumerate(json):
        dataset = 'values ('
        insertQuery = 'INSERT INTO alpl_prod.alpl_unit_dept_mapping ('
        # updateQuery = 'UPDATE alpl_prod.alpl_unit_dept_mapping SET '
        j = 0
        flag = 0
        value = '0'
        try:
            for table_column, json_column in column_map.items():
                if j == 0:
                    insertQuery = insertQuery + table_column
                    dataset = dataset + validate_string_unit_dept(item.get(json_column, None), table_column)
                else:
                    insertQuery = insertQuery + ',' + table_column
                    dataset = dataset + ',' + validate_string_unit_dept(item.get(json_column, None), table_column)

                j = 1
            dataset = dataset + ')'
            insertQuery = insertQuery + ')'
            # updateQuery = updateQuery + ' where alpl_emp_id=' + value
            totalQuery = insertQuery + dataset
            # if flag:
            #     ucnt = ucnt + 1
            #     cursor.execute(updateQuery)
            # else:
            icnt = icnt + 1
            # logger.info(totalQuery)
            cursor.execute(totalQuery)

        except Exception as e:
            logger.info(e)
            logger.info(totalQuery)
            conn.rollback()

            try:
                Util.insert_error_record('alpl_unit_dept_mapping', source, item, totalQuery, str(e))
            except Exception as exp:
                logger.error(str(exp))
            else:
                conn.commit()
                errors = errors + 1
        else:
            conn.commit()
            p = p + 1
    conn.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_unit_dept_mapping:", p)
    logger.info("inserted :", icnt)
    logger.info("updated :", ucnt)
    logger.info("connection closed successfully")


def load_asset_model(jsonData, source, conn):
    logger.info("Data ingestion starting for alpl_asset_model")
    cursor = conn.cursor()

    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'

    column_map = {'alpl_asset_model_id': 'VEH_MODEL_ID',
                  'asset_model_name': 'VEH_NAME',
                  'asset_manufacturer': 'MANUFACTURER',
                  'max_load_capacity': 'MAX_LOAD_CAPACITY',
                  'max_return_load_capacity': 'MAX_RETURN_LOAD_CAPACITY'
                  }

    cursor.execute('SELECT alpl_asset_model_id from alpl_asset_model')
    result = cursor.fetchall()
    assetIdList = [i[0] for i in result]
    p = 0
    ctu = 0
    cti = 0
    errors = 0
    for i, item in enumerate(jsonData):
        dataset = 'values ('
        insertQuery = 'INSERT INTO alpl_asset_model ('
        updateQuery = 'UPDATE alpl_asset_model SET '
        i = 0
        flag = 0
        valueID = '0'
        value = None
        q = None
        modelid=item.get('VEH_MODEL_ID',None)
        try:
            for table_column, json_column in column_map.items():

                if (flag == 1) or (table_column == 'alpl_asset_model_id' and (
                        int(validate_string(item.get(json_column, None), table_column)) in assetIdList)):
                    if flag == 0:
                        if (value == None):
                            value = ""
                        if (type(value) in (int, float)):
                            value = str(value)
                        valueID = int(validate_string(item.get(json_column, None), table_column))
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
                cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                cti = cti + 1
            conn.commit()

        except Exception as e:
            logger.info(e)
            conn.rollback()
            try:
                Util.insert_error_record('alpl_driver_list_master', source, item, q, str(e),filename,modelid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                conn.commit()
                errors = errors + 1

        else:
            conn.commit()
            p = p + 1
    cursor.close()
    conn.close()
    logger.info("connection closed successfully")
    logger.info("update :", ctu)
    logger.info("insert :", cti)
    logger.info("no of rows processed are :", p)
    logger.info("errors :", errors)

def load_ppm_schedule_master(json,source,con):
    logger.info("Data ingestion starting for alpl_ppm_schedule_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()

    src = 'logistics'

    cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_prod.alpl_unit_master')
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
    model_column_map = {'model':'MODEL',
			            'ppm_schedule':'OPERATIONKEY',
			            'sch_freq':'SCHFREQUENCY_KMS',
			            'activity':'ACTTEXT'}
    material_column_map = {'part_id':'MATERIAL',
                           'part_quantity':'QUANTITY',
                           'activity':'ACTTEXT',
                           'ppm_schedule':'OPERATIONKEY',
                           'model':'GROUP',
                           'unit': 'PLANT'
                            }
    if (source=='PPM_MATERIAL'):
        column_map=material_column_map
    else:
        column_map=model_column_map


    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_prod.alpl_ppm_schedule_master ('
        # updateQuery = 'UPDATE alpl_prod.alpl_ppm_schedule_master SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        data = {}
        try:
            for table_column, json_column in column_map.items():
                if source=='PPM_MATERIAL':
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
                        if (table_column == 'unit'):
                            if (value != 'ALPL'):
                                value = dictUnitc.get(int(value))
                            else:
                                value = ''
                        if (table_column=='model'):
                            value=value[4:]

                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""

                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1
                else:
                    pass
            dataset = dataset + ')'
            query = query + ')'
            # updateQuery = updateQuery + ' where job_card_id=' + valueID
            totalQuery = query + dataset

            q = totalQuery
            # cursor.execute(totalQuery)
            cti = cti +1
            if source == 'PPM_MATERIAL':
                cursor.execute(totalQuery)
            else:
                existQuery='update alpl_ppm_schedule_master set sch_freq='+validate_string(item.get('SCHFREQUENCY_KMS', None),'SCHFREQUENCY_KMS')+' where model='+"'"+validate_string(item.get('MODEL', None),'MODEL')+"'"+' and ppm_schedule='+validate_string(item.get('OPERATIONKEY', None),'OPERATIONKEY')+' and activity='+validate_string(item.get('ACTTEXT', None),'ACTTEXT')
                cursor.execute(existQuery)


        except Exception as e:

            logger.info(e)

            con.rollback()

            try:

                Util.insert_error_record('alpl_ppm_schedule_master', source, item, q, str(e), filename)

            except Exception as exp:

                logger.error(str(exp))

            else:

                con.commit()

                errors = errors + 1

        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_job_Card_trans:"+str(p))
    logger.info("updated rows :"+str(ctu))
    logger.info("inserted rows :"+ str(cti))
    logger.info("Error rows :"+str(errors))
    logger.info("connection closed successfully")


def load_parts_master(json, source,con):
    logger.info("Data ingestion starting for alpl_parts_master")
    cursor = con.cursor()

    src = 'logistics'
    unitDictionary=get_units(cursor)
    cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_prod.alpl_unit_master')
    result = cursor.fetchall()
    unitCodeList = [i[0] for i in result]
    unitIdList = [i[1] for i in result]

    dictUnitc = {}
    length = len(unitCodeList)
    for x in range(length):
        dictUnitc[unitCodeList[x]] = unitIdList[x]

    cursor.execute('SELECT unit_id,id FROM alpl_prod.alpl_unit_dept_mapping where dept_id = 4')
    result = cursor.fetchall()
    unitList = [i[0] for i in result]
    udIdList = [i[1] for i in result]

    dictUDId = {}
    length = len(unitList)
    for x in range(length):
        dictUDId[unitList[x]] = udIdList[x]

    cursor.execute('SELECT location,parts_location_master_id FROM alpl_prod.alpl_parts_location_master')
    result = cursor.fetchall()
    locationList = [i[0] for i in result]
    locationIdList = [i[1] for i in result]

    dictLoc = {}
    length = len(locationList)
    for x in range(length):
        dictLoc[locationList[x]] = locationIdList[x]

    cursor.execute('select concat(batch,alpl_part_id,store_id,unit_id) from  alpl_prod.alpl_parts_master ')
    res = cursor.fetchall()
    partsLists = [i[0] for i in res]
    ctu = 0
    cti = 0
    p = 0
    errors = 0

    columMap = {'alpl_part_id': 'PART_ID',
                'batch': 'BATCH',
                'store_id': 'STORE_ID',
                # 'unit_dept_id': 'UNIT_NUM',
                'part_name': 'PART_NAME',
                'con_barcode': 'CON_BARCODE',
                'part_type': 'PART_TYPE',
                'part_num': 'PART_NUM',
                'part_category': 'PART_CATG',
                'part_category_desc': 'PART_CATG_DESC',
                'part_type_desc': 'PART_TYPE_DESC',
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
                'part_sub_cat_group': 'AGG_GRP',
                'part_sub_cat_code': 'PART_SUB_CATG',
                'purchasing_group_code': 'PUR_GROUP',
                'purchasing_group_desc': 'PUR_GRP_DESC',
                'warrenty_end_date':'WARRENTY_EDATE',
                'warrenty_start_date':'WARRENTY_SDATE',
                'target_life':'KMS'
                }

    for i, item in enumerate(json):
        dataset = ' values ('
        insertQuery = 'INSERT INTO alpl_prod.alpl_parts_master ('
        updateQuery = 'UPDATE alpl_prod.alpl_parts_master SET '
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
            if (storeId == None):
                storeId = ""

            unitId = item.get('UNIT_NUM', None)
            unitId = getUnitId(unitId,unitDictionary)

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

                        # if (table_column == 'unit_dept_id'):
                        #     value = unitId
                        #     if (unitId == None):
                        #         unitId = ""
                        #
                        #     if (type(unitId) in (int, float)):
                        #         unitId = str(unitId)
                        #     unitId = validate_string(unitId, table_column)

                        if (table_column == 'store_id'):
                            value = storeId
                            if (storeId == None):
                                storeId = ""

                            if (type(storeId) in (int, float)):
                                storeId = str(storeId)
                            if (storeId == None):
                                storeId = ""
                            storeId = validate_string(storeId, table_column)

                        if (table_column == 'batch' or batch == ''):
                            if (batch == ''):
                                batch = ""
                                batch = validate_string(batch, table_column)

                            batch = "'" + str(batch) + "'"

                        if (table_column == 'unit_id'):
                            value = getUnitId(value,unitDictionary)

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

                        # if (table_column == 'unit_dept_id'):
                        #     value = unitId
                        #     if (unitId == None):
                        #         unitId = ""
                        #
                        #     if (type(unitId) in (int, float)):
                        #         unitId = str(unitId)
                        #     unitId = validate_string(unitId, table_column)

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

                        if (table_column == 'unit_id'):
                            value = getUnitId(value,unitDictionary)

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
            updateQuery = updateQuery + ' where alpl_part_id=' + valueID + ' and batch = ' + str(
                batch) + ' and store_id =' + str((storeId))
            updateQuery = updateQuery + ' and unit_id = ' + str(unitId)
            totalQuery = insertQuery + dataset
            unitt=item.get('UNIT_NUM', None)
            # if '20' not in item.get('UNIT_NUM', None):
            if flag:
                q = updateQuery
                cursor.execute(updateQuery)
                ctu = ctu + 1

            else:
                q = totalQuery
                cursor.execute(totalQuery)
                cti = cti + 1


        except Exception as e:

            logger.info(e)

            con.rollback()

            try:

                Util.insert_error_record('alpl_parts_master', source, item, q, str(e), filename)

            except Exception as exp:

                logger.error(str(exp))

            else:

                con.commit()

                errors = errors + 1
        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_parts_master:"+ str(p))
    logger.info("connection closed successfully")
    logger.info("inserted rows:"+str(cti))
    logger.info("updated rows:"+ str(ctu))
    logger.info("Errors :"+ str(errors))

def godown_stock_mst_trans(json, source,con):
    logger.info("Data ingestion starting for alpl_godown_master")
    # json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'
    p = 0
    errors = 0
    column_map = {
        'rcl_cust_id': 'RCL_CustID',
        'location_id': 'LocationID',
        'grade': 'Grade',
        'msl':'MSL',
        'capacity':'Capacity',
        'stock_level': 'StockLevel',
        'stock_value': 'StockValue',
        'last_updated_time': 'CreatedDateTime'

    }
    uct = 0
    ict = 0
    empIdQuery = 'select concat(rcl_cust_id,location_id,grade) from alpl_prod.alpl_godown_master'
    cursor.execute(empIdQuery)
    result = cursor.fetchall()
    stockList = [i[0] for i in result]

    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_prod.alpl_godown_master ('
        updateQuery = 'UPDATE alpl_prod.alpl_godown_master SET '
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

        cust_id = item.get('RCL_CustID', None)
        if (cust_id == ''):
            cust_id = ''
        else:
            cust_id = str(cust_id)

        loc_id = item.get('LocationID', None)
        if (loc_id == ''):
            loc_id = ''
        else:
            loc_id = str(loc_id)

        grd = item.get('Grade', None)
        if (grd == ''):
            grd = ''
        else:
            grd = str(grd)

        checkValue = cust_id+loc_id+grd


        try:
            for table_column, json_column in column_map.items():

                if (flag == 1) or ( table_column=='rcl_cust_id' and ((checkValue) in stockList)):
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

                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    i = 1
                else:
                    if (i == 0):
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




            updateQuery = updateQuery + ' where rcl_cust_id = ' + str(validate_string(cust_id,'table_col')) + ' and location_id = ' + str(
                loc_id) + ' and grade= ' + str(validate_string(grd,'tab_col'))

            if (flag == 1):
                q = updateQuery
                cursor.execute(updateQuery)
                uct = uct + 1
            else:
                dataset = dataset + ')'
                query = query + ')'
                query = query + dataset
                cursor.execute(query)
                ict = ict+1

        except Exception as e:
            logger.info(str(e) + str(i))
            logger.info(q)
            con.rollback()

            try:
                Util.insert_error_record('alpl_godown_trans', source, item, q, str(e), filename,cust_id)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_godown_master:"+ str(p))
    logger.info("inserted :"+str(ict))
    logger.info("upated :"+ str(uct))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")


def load_asset_master(jsonData, source, conn):
    logger.info("Data ingestion starting for asset_master")
    columMap = {
        'asset_reg_number': 'TRUCK_NO',
        # 'refid': 'ALPLREFID',
        'asset_capacity': 'CAPACITY',
        'asset_type': '',
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
        'asset_manufacturer': 'MANUFACTURER',
        'asset_name': 'TRUCK_NAME',
        'usage_ind': 'USAGE_IND',
        'COMMERCIAL_NONCommercial': 'USAGE_IND_TEXT',
        'temp_check': ''
    }

    cursor = conn.cursor()
    cursor.execute('SELECT asset_reg_number from alpl_asset_master')
    result = cursor.fetchall()
    regList = [i[0] for i in result]

    cursor.execute('SELECT alpl_asset_model_id,asset_model_id FROM alpl_asset_model')
    result_am = cursor.fetchall()
    model_map = dict(result_am)

    cursor.execute('SELECT alpl_asset_model_id,max_load_capacity FROM alpl_asset_model')
    result_tc = cursor.fetchall()
    tonnage_map = dict(result_tc)

    dictUnitc = get_units(cursor)

    ctu = 0
    cti = 0
    p = 0
    errors = 0
    src = 'logistics'
    orgId = 1
    if ("MINING" in source):
        src = 'mining'
        orgId = 2

    for i, item in enumerate(jsonData):
        dataset = 'values ('
        query = 'INSERT INTO alpl_asset_master ('
        updateQuery = 'UPDATE alpl_asset_master SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        asset = item.get('TRUCK_NO',None)
        #insertQueryTele = 'insert into alpl_asset_telemetry(asset_reg_number,message_timestamp,status) values('+ "'"+str(asset)+"',"+"'"+str(0)+"',"+"'"+"UNASSIGNED VEHICLE"+"');"
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
                            value = getUnitId(value, dictUnitc)
                        if (table_column == 'temp_check'):
                            value = 1
                        if (table_column == 'asset_type'):
                            value = 'T'

                        if (table_column == 'asset_model_id'):
                            value = item.get(json_column, None)
                            value = model_map.get(int(value))
                        if (table_column == 'tonnage'):
                            value = tonnage_map.get(int(value))

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
                            value = getUnitId(value, dictUnitc)
                        if (table_column == 'asset_type'):
                            value = 'T'

                        if (table_column == 'temp_check'):
                            value = 1

                        if (table_column == 'asset_model_id'):
                            value = item.get(json_column, None)
                            value = model_map.get(int(value))

                        if (table_column == 'tonnage'):
                            value = tonnage_map.get(int(value))
                        if (value == None):
                            value = ""

                        if (type(value) in (int, float)):
                            value = str(value)

                        dataset = dataset + ',' + validate_string(value, table_column)

                    i = 1
            dataset = dataset + ')'
            query = query + ')'
            updateQuery = updateQuery + ' ,last_updated_time=now() where asset_reg_number=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                q = updateQuery
                cursor.execute(updateQuery)
                ctu = ctu + 1
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                #cursor.execute(insertQueryTele)
                cti = cti + 1
            conn.commit()

        except Exception as e:
            logger.info(e)
            logger.info(q)
            conn.rollback()

            try:
                Util.insert_error_record('alpl_asset_master', source, item, q, str(e),filename,asset)
            except Exception as exp:
                logger.error(str(exp))
            else:
                conn.commit()
                errors = errors + 1

        else:
            conn.commit()
            p = p + 1
    cursor.close()
    conn.close()
    logger.info("inserted rows:", cti)
    logger.info("updated rows:", ctu)
    logger.info("errors :", errors)
    logger.info("connection asset_master closed successfully and no of rows inserted:", p)


def plant_master(json, source,conn):
    logger.info("Data ingestion starting for alpl_plant_master")
    # json_obj = getJsonFile(json,source)

    cursor = conn.cursor()
    cursor.execute("select alpl_plant_code from alpl_plant_master")
    result = cursor.fetchall()
    plants = [i[0] for i in result]
    errors = 0
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'

    ctu = 0
    cti = 0
    p = 0
    q = None
    plant=None
    columMap = {
        'alpl_plant_code': 'PLANT_AREA',
        'plant_name': 'UNIT_NAME',
        'hod': 'HOD',
        'hod_name': 'HOD_NAME',
        'type': 'ZUNIT_TYPE',
        'status': 'STATUS',
        'unit_code': 'UNIT'
    }

    for i, item in enumerate(json):
        plant = str(item.get('PLANT_AREA',None))
        try:
            dataset = 'values ('
            query = 'INSERT INTO alpl_plant_master ('
            update_qry='update alpl_plant_master set '
            i = 0
            godown = item.get('ZUNIT_TYPE', None)
            if (godown == ''):
                godown = ''
            else:
                godown = str(godown)

            if(int(item.get('PLANT_AREA',None)) in plants):
                    update_qry = update_qry+'hod='+"'"+str(item.get('HOD', 'NULL'))+"', "
                    update_qry = update_qry+'hod_name='+"'"+item.get('HOD_NAME','NULL')+"', "
                    update_qry = update_qry + 'plant_name=' + "'" + item.get('UNIT_NAME', 'NULL') + "', "
                    update_qry = update_qry+'type='+"'"+item.get('ZUNIT_TYPE','NULL')+"', "
                    update_qry = update_qry+'status='+"'"+item.get('STATUS','NULL')+"', "
                    update_qry = update_qry+'unit_code='+"'"+item.get('UNIT','NULL')+"'"
                    update_qry = update_qry + ' where alpl_plant_code=' + "'" + str(item.get('PLANT_AREA', 'NULL')) + "'"
                    cursor.execute(update_qry)
            else:
                for table_column, json_column in columMap.items():
                    if i == 0 and (godown != 'UNIT'):
                        query = query + table_column
                        value = item.get(json_column, None)
    
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + validate_string(value, table_column)
                    elif (godown != 'UNIT'):
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
                if (godown != 'UNIT'):
                    cursor.execute(totalQuery)
        except Exception as e:
            logger.info(e)
            logger.info(q)
            conn.rollback()
            try:
                Util.insert_error_record('alpl_plant_master', source, item, totalQuery, str(e), filename,plant)
            except Exception as exp:
                logger.error(str(exp))
            else:
                conn.commit()
                errors = errors + 1

        else:
            conn.commit()
            p = p + 1
    cursor.close()
    conn.close()
    logger.info("inserted rows:"+str(p))
    logger.info("errors :"+str(errors))
    logger.info("connection plant_master closed successfully and no of rows inserted:"+str(p))



def load_tyres_trans(json,source,con):
    logger.info("Data ingestion starting for tyres_trans")
    columMap1 = {
        'tyre_no':'TYRE',
        'tyre_position':'POSITION',
        'fitment_date':'FIT_DATE',
        # 'CFITMENT_DATE':'CFITMENT_DATE',
        # 'RDATE_T':'REMOVAL_DATE',
        'removal_date':'REMOVAL_DATE',
        # 'tyre_purchase_date':'TYRE_PURCH_DATE',
        'asset_reg_number':'TRUCK_NO',
        # 'trip_id':'TRIP_ID',
        'store_loc':'CURRENT_STOREID',
        'tyre_act_scra': 'TYRE_ACT_SCRA',
        # 'retreader':'RETADER_NAME',
        # 'TYRE_ACT_SCRA_T':'TYRE_ACT_SCRA',
        # 'TARGET_KMS_T':'TARGET_KMS',
        'tyre_stage':'STAT',
        'tyre_cost':'COST',
        # 'grn_date':'GRN_DATE',
        # 'unit_no':'PLANT',
        'trans_unit': 'PLANT',
        'retreader': 'RETADER_NAME',
        "retreader_type":"RETADER_TYPE",
        "retreader_pattern":"RETADER_PATTERN",
        "grn_date":"GRN_DATE",
        # "MATERIAL_T":"MATERIAL",
        # "Batch_T":"BATCH",
        # "Mileage":"TOTAL_MILEAGE",
        "total_kms":"TOTAL_MILEAGE"
        # "Reason":"REASON"
    }
    columMap2={
        'tyre_no': 'TYRE_NO',
        'tyre_type': 'TYRE_TYPE',
        'tyre_model': 'TYRE_MODEL',
        'manufacture': 'MANUFACTURE',
        'tyre_purchase_date': 'TYRE_PURCH_DATE',
        'tyre_war_valid_date': 'TYRE_WAR_VALID_DATE',
        'tyre_size': 'TYRE_SIZE',
        'retreader_type': 'TYRE_RETRE_PAT_TYPE',
        'unit_no': 'UNIT_NO',
        'trans_unit': 'UNIT_NO',
        'tyre_pattern': 'PATTERN_TYRE',
        'tyre_stage': 'TYRE_STATUS',
        'tyre_act_scra': 'TYRE_ACT_SCRA',
        'target_kms': 'TARGET_KMS',
        'tyre_cost': 'TYRE_COST',
        'grn_date': 'GRNDATE',
        'total_kms': 'TYRE_KMS',
        'current_master_kms':'TYRE_KMS',
        'asset_reg_number': 'TRUCK_NO',
        'tyre_position': 'POSITION',
        'store_loc': 'STORE_LOC',
        'retreader': 'RETRADER_NAME'
    }
    columMap3 = {
        'tyre_no': 'TYRE',
        'store_loc': 'CURRENT_STOREID',
        'tyre_act_scra': 'TYRE_ACT_SCRA',
        "grn_date": "GRN_DATE",
        'trans_unit': 'PLANT',
        'retreader': 'RETADER_NAME',
        "retreader_type": "RETADER_TYPE",
        "retreader_pattern": "RETADER_PATTERN"
    }
    # print(source)
    if(source=='TYRES_MASTER'):
        columMap=columMap2
    elif (source == 'TYRE_HISTORY'):
        columMap = columMap1
    else:
        columMap = columMap3

    # print(columMap)
    cursor = con.cursor()
    cursor.execute('SELECT tyre_no from alpl_tyres_stage_master')
    result = cursor.fetchall()
    tyreList = [i[0] for i in result]
    # print(tyreList)
    dictUnitc = get_units(cursor)
    ctu = 0
    cti= 0
    upM=0
    p = 0
    errors = 0
    src = 'logistics'
    orgId = 1
    if ("MINING" in source):
        src = 'mining'
        orgId = 2

    for i, item in enumerate(json):
        dataset = ' values ('
        query = 'INSERT INTO alpl_tyres_stage_master ('
        updateQuery = 'UPDATE alpl_tyres_stage_master SET '
        i = 0
        flag = 0
        valueID = '0'
        stageID='0'
        truck='0'
        serialID=0
        kms=0
        q = None
        tyre=""
        if(source=='TYRES_MASTER'):
            tyre = item.get('TYRE_NO')
        else:
            tyre = item.get('TYRE');
        try:
            for table_column, json_column in columMap.items():
                # print(table_column,item.get(json_column, None))
                if table_column!='tyre_position' and table_column!='fitment_date' and table_column!='removal_date' and table_column!='asset_reg_number' and table_column!='total_kms':
                    if (flag == 0 or (table_column == 'tyre_no' and (item.get(json_column, None) in tyreList))):
                        # if flag == 0:
                        #     val = item.get(json_column, None)
                        #     if (type(val) in (int, float)):
                        #         val = str(val)
                        #     if (val == None):
                        #         val = ""


                        if i == 0:
                            query = query + '"'+table_column+'"'
                            value = item.get(json_column, None)


                            if (type(value) in (int, float)):
                                value = str(value)

                            dataset = dataset + validate_string(value, table_column)
                            if (table_column=='tyre_stage'):
                                    stageID=validate_string(value, table_column)

                            if (table_column == 'tyre_no'):
                                valueID = validate_string(value, table_column)
                            updateQuery = updateQuery + '"' + table_column + '"'
                            # value = item.get(json_column, None)
                            #
                            # if (value == None):
                            #     value = ""
                            # if (type(value) in (int, float)):
                            #     value = str(value)
                            updateQuery = updateQuery + '=' + validate_string(value, table_column)

                        # if (i == 0):
                        #     updateQuery = updateQuery + '"'+table_column+'"'
                        #     value = item.get(json_column, None)
                        #
                        #     if (value == None):
                        #         value = ""
                        #     if (type(value) in (int, float)):
                        #         value = str(value)
                        #     updateQuery = updateQuery + '=' + validate_string(value, table_column)
                        #     if (table_column=='tyre_stage'):
                        #         stageID=validate_string(value, table_column)
                        else:
                            updateQuery = updateQuery + ',' + '"'+table_column+'"'
                            value = item.get(json_column, None)

                            if (table_column == 'unit_no'):
                                value = getUnitId(value,dictUnitc)


                            if (value == None):
                                value = ""

                            if (type(value) in (int, float)):
                                value = str(value)

                            updateQuery = updateQuery + '=' + validate_string(value, table_column)
                            if (table_column=='tyre_stage'):
                                stageID=validate_string(value, table_column)
                            if (table_column == 'tyre_no'):
                                valueID = validate_string(value, table_column)
                            query = query + ',' + '"'+table_column+'"'
                            value = item.get(json_column, None)

                            if (table_column == 'unit_no'):
                                value = getUnitId(value,dictUnitc)


                            if (value == None):
                                    value = ""

                            if (type(value) in (int, float)):
                                value = str(value)

                            dataset = dataset + ',' + validate_string(value, table_column)
                            if (table_column=='tyre_stage'):
                                stageID=validate_string(value, table_column)

                        i = 1
                    else:
                        pass
                        # if i == 0:
                        #     query = query + '"'+table_column+'"'
                        #     value = item.get(json_column, None)
                        #
                        #
                        #     if (type(value) in (int, float)):
                        #         value = str(value)
                        #
                        #     dataset = dataset + validate_string(value, table_column)
                        #
                        # else:
                        #     query = query + ',' + '"'+table_column+'"'
                        #     value = item.get(json_column, None)
                        #
                        #     if (table_column == 'unit_no'):
                        #         value = getUnitId(value)
                        #
                        #
                        #     if (value == None):
                        #             value = ""
                        #
                        #     if (type(value) in (int, float)):
                        #         value = str(value)
                        #
                        #     dataset = dataset + ',' + validate_string(value, table_column)
                        #
                        #
                        # i = 1
            if (source == 'TYRES_MASTER'):
                query=query +',active_inactive'
                dataset = dataset + ","+str(1)
            # if (source == 'TYRE_HISTORY'):
            #     query=query +',active_inactive'
            #     dataset = dataset + ","+str(1)
            dataset = dataset + ')'
            dataset=dataset+' returning id'
            query = query + ')'
            if (source != 'TYRE_INTRANSIT'):
                updateQuery = updateQuery + ", updated_time=now() where tyre_no=" + valueID + ' and tyre_stage=' + stageID
            else:
                updateQuery = updateQuery + ", updated_time=now() where tyre_no=" + valueID + ' and active_inactive=' + '1'
                        # +' and "active_inactive"=0'
            totalQuery = query + dataset
            if (flag == 0) and source!='TYRE_INTRANSIT':
                # updateRow='UPDATE alpl_tyres_new set "ACTIVE_INACTIVE"=0 where "TYRE_NO"='+valueID+' and "ACTIVE_INACTIVE"=1'
#                 insertRow='insert into alpl_tyres_stage_master(tyre_no, tyre_type, tyre_model, manufacture, tyre_purchase_date, tyre_war_valid_date, tyre_size, unit_no, tyre_pattern, tyre_cost, tyre_stage, target_kms, store_loc, tyre_act_scra, grn_date, active_inactive, retreader, retreader_type, retreader_pattern) '+
# 'select m.tyre_no,m. tyre_type,m. tyre_model,m. manufacture,m. tyre_purchase_date,m. tyre_war_valid_date,m. tyre_size,m. unit_no,m. tyre_pattern,m. tyre_cost,m. tyre_stage,m. target_kms,m. store_loc,m. tyre_act_scra,m. grn_date,m. 0,m. retreader,m. retreader_type,m. retreader_pattern from alpl_tyres_stage_master m where m.tyre_no='+valueID+' and m.active_inactive is null returning id'
                # checkQ='select TYRE_NO from alpl_tyres_new where TYRE_NO='+valueID+' and POTION_TYRE_T='+item.get('POTION_TYRE',None)+' and TYRE_STATUS_T='+item.get('TYRE_STATUS',None)
                # result=cursor.execute(checkQ)
                # if result==None:
                # cursor.execute(updateRow)
                # cursor.execute(insertRow)
                # print(updateRow)
                # print(totalQuery)
                selectRow='select id from alpl_tyres_stage_master where tyre_no='+valueID+' and tyre_stage='+stageID
                # print(selectRow)
                cursor.execute(selectRow)
                selectResult=cursor.fetchall()
                # print(selectResult)
                if selectResult:
                    serialID=selectResult[0][0]
                    # print(updateQuery)
                    cursor.execute(updateQuery)
                if not selectResult:
                    # print(totalQuery)
                    # masterupdate = 'update alpl_tyres_stage_master set active_inactive=' + '0' + ' where tyre_no=' + valueID
                    # cursor.execute(masterupdate)
                    cursor.execute(totalQuery)

                    selectResult=cursor.fetchall()
                    # tyremodelUpdate="update alpl_tyres_stage_master u set tyre_type=m.tyre_type,tyre_model=m.tyre_model,manufacture=m.manufacture,tyre_purchase_date=m.tyre_purchase_date , tyre_war_valid_date=m.tyre_war_valid_date ,tyre_size=m.tyre_size, unit_no=m.unit_no,tyre_pattern=m.tyre_pattern, target_kms=m.target_kms from alpl_tyres_stage_master m where m.tyre_no=u.tyre_no and m.tyre_no="+valueID+" and m.active_inactive="+'0'+" and u.active_inactive="+'1'
                    # cursor.execute(tyremodelUpdate)
                    if selectResult:
                        serialID=selectResult[0][0]
                        # updateMaster = 'update alpl_tyres_stage_master u set tyre_type=m.tyre_type,tyre_model=m.tyre_model,manufacture=m.manufacture,tyre_purchase_date=m.tyre_purchase_date , tyre_war_valid_date=m.tyre_war_valid_date ,tyre_size=m.tyre_size, tyre_pattern=m.tyre_pattern, target_kms=m.target_kms from alpl_tyres_stage_master m where m.rec_type='+"'M'"+' and m.tyre_no='+valueID+' and m.tyre_stage='+stageID+' and u.id='+str(serialID)
                        # upM = upM + 1
                        # cursor.execute(updateMaster)
                        # print(updateMaster)
                    # print(serialID)
                    # trackQ="insert into alpl_tyres_kms_hrs(tyre_stage_id,asset_reg_number,tyre_position,fitment_date,removal_date) values("+serailID+','+validate_string(item.get('TRUCK_NO', None),'asset_reg_number')+','+validate_string(item.get('POSITION', None),'tyre_position')+','+validate_string(item.get('FIT_DATE', None),'fitment_date')+','+validate_string(item.get('REMOVAL_DATE', None),'removal_date')+')'
                    # print(trackQ)
                    # cursor.execute(trackQ)
                # print(q)
                    # print(updateQuery)
                    # cursor.execute(updateQuery)
                    cti = cti + 1
                # print(serialID)
                # print(validate_string(item.get('TYRE_KMS', None),'total_kms'),'kms')
                selectKmsQuery="select total_kms from alpl_tyre_tracking where tyre_stage_id="+str(serialID)+" and (asset_reg_number="+validate_string(item.get('TRUCK_NO', None),'asset_reg_number')+") and (tyre_position="+validate_string(item.get('POSITION', None),'tyre_position')+")"
                cursor.execute(selectKmsQuery)
                selectResult=cursor.fetchall()
                # print(selectResult)
                if (source == 'TYRES_MASTER'):
                    cti=cti
                    # if selectResult:
                    #     kms=float(selectResult[0][0])
                    #     # print(kms,"kms")
                    #     if float(validate_string(item.get('TYRE_KMS', None),'total_kms'))>=kms:
                    #         if item.get('TRUCK_NO', None) != '' and item.get('POSITION', None) != '':
                    #             trackQ="update alpl_tyre_tracking set total_kms="+validate_string(item.get('TYRE_KMS', None),'total_kms')+",last_updated_time=now() where tyre_stage_id="+str(serialID)+" and asset_reg_number="+validate_string(item.get('TRUCK_NO', None),'asset_reg_number')+" and tyre_position="+validate_string(item.get('POSITION', None),'tyre_position')
                    #             # print(trackQ)
                    #             cursor.execute(trackQ)
                    #         # cursor.execute(updateQuery)
                    # if not selectResult:
                    #     if item.get('TRUCK_NO', None) != '' and item.get('POSITION', None) != '':
                    #         trackQ="insert into alpl_tyre_tracking(tyre_stage_id,asset_reg_number,tyre_position,total_kms) values("+str(serialID)+','+validate_string(item.get('TRUCK_NO', None),'asset_reg_number')+','+validate_string(item.get('POSITION', None),'tyre_position')+','+validate_string(item.get('TYRE_KMS', None),'total_kms')+')'
                    #         # print(trackQ)
                    #         cursor.execute(trackQ)
                    #     # cursor.execute(updateQuery)
                else:
                    if selectResult:
                        kms = float(selectResult[0][0])
                        new_kms = kms + int(validate_string(str(item.get('TOTAL_MILEAGE', None)), 'total_kms'))
                        # if float(validate_string(str(item.get('TOTAL_MILEAGE', None)), 'total_kms')) > kms:
                        trackQ = "update alpl_tyre_tracking set fitment_date=" + validate_string(
                            str(item.get('FIT_DATE', None)), 'fitment_date') + ", removal_date=" + validate_string(
                            str(item.get('REMOVAL_DATE', None)), 'removal_date')+", asset_reg_number="+validate_string(str(item.get('TRUCK_NO', None)),
                                                          'asset_reg_number') + ', tyre_position=' + validate_string(
                        str(item.get('POSITION', None)), 'tyre_position') + ", total_kms=" + str(new_kms) +", last_updated_time=now() where tyre_stage_id=" + str(serialID)+" and (asset_reg_number is null or asset_reg_number="+validate_string(item.get('TRUCK_NO', None),'asset_reg_number')+") and (tyre_position is null or tyre_position="+validate_string(item.get('POSITION', None),'tyre_position')+")"
                        cursor.execute(trackQ)
                            # print(trackQ)
                    if not selectResult:
                        trackQ = "insert into alpl_tyre_tracking(tyre_stage_id,asset_reg_number,tyre_position,fitment_date,removal_date,total_kms) values(" + str(
                            serialID) + ',' + validate_string(str(item.get('TRUCK_NO', None)),
                                                              'asset_reg_number') + ',' + validate_string(
                            str(item.get('POSITION', None)), 'tyre_position') + ',' + validate_string(
                            str(item.get('FIT_DATE', None)), 'fitment_date') + ',' + validate_string(
                            str(item.get('REMOVAL_DATE', None)), 'removal_date') + ',' + validate_string(
                            str(item.get('TOTAL_MILEAGE', None)), 'total_kms') + ')'
                        # print(trackQ)
                        cursor.execute(trackQ)
            elif source=='TYRE_INTRANSIT':
                # q = totalQuery
                # print(q)
                ctu = ctu+1
                # raise Exception(totalQuery)
                cursor.execute(updateQuery)
            else:
                # q = totalQuery
                # print(q)
                ctu = ctu+1
                # raise Exception(totalQuery)
                # cursor.execute(totalQuery)

        except Exception as e:
            logger.info(e)
            logger.info(q)
            con.rollback()

            try:
                Util.insert_error_record('alpl_tyres_stage_master', source, item, q, str(e),filename,tyre)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1

        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("inserted rows:"+str(cti))
    logger.info("updated rows:"+str(ctu))
    logger.info("errors :"+str(errors))
    logger.info("update M:"+str(upM))
    logger.info("connection tyre_trans closed successfully and no of rows inserted:"+str(p))
    

def main(file_name):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # file = event
    # file = os.path.abspath('E:/prod_dump') + "/model.json"
    try:
        # json_data = open(file).read()
        # json_obj = json.loads(json_data)
        global filename
        filename=file_name
        bkt = Util.sap_master_bucket
        if('tyre' in file_name):
            bkt=Util.tyres_parts_bucket
        elif('parts' in file_name):
            bkt = Util.tyres_parts_bucket
        elif('PPM' in file_name):
            bkt = Util.tyres_parts_bucket
        json_obj = Util.getJsonFile(bkt, file_name)
        logger.info("Processing file: " + file_name)

        data = str(json_obj)
        ch = data.find('[')
        dataKey = data[ch - 7:ch - 3]
        data = data[2:8]
        dest_bucket = ''
        sourcetype='PORTAL'
        if (data == 'Source'):
            dataSource = 'data'
            dest_bucket = Util.portal_processed_bucket
        elif (data == 'SOURCE'):
            dataSource = 'DATA'
            dest_bucket = Util.sap_processed_bucket
            sourcetype='SAP'
        jsonData = json_obj[dataKey]
        source = json_obj[data]
        func = None
        if source == 'TRUCK_MODEL':
            func = load_asset_model
        elif source == 'TRUCK_MASTER':
            func = load_asset_master
        elif source == 'DEPT_DATA':
            func = load_unit_dept_mapping
        elif source == 'UNIT_DATA':
            func = plant_master
        elif source == 'TYRES_MASTER' or source == 'TYRE_HISTORY' or source=='TYRE_INTRANSIT':
            func = load_tyres_trans
        elif source=='PARTS_MASTER':
            func=load_parts_master
        elif source== 'PPM_MODEL' or source=='PPM_MATERIAL':
            func=load_ppm_schedule_master
        elif source == 'GODOWNSTOCK_TRANS':
            func = godown_stock_mst_trans
        if func != None:
            MultiProc.runDataIngestion(jsonData, func, source)
        Util.move_blob(bkt, file_name, dest_bucket, file_name)
        #Util.call_error_data_url(filename, sourcetype)
    except Exception as ex:
        logger.info("Halting json data load processing due to following exception:" + str(ex))
        Util.move_blob(bkt, file_name, Util.error_bucket, file_name)