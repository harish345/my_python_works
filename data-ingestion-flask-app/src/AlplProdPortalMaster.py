from datetime import datetime
import MultiProc
import Util
from LoggingUtil import logger


filename=''

def get_location_type(cursor):
    global loct_map
    cursor.execute("SELECT alpl_reference_id,alpl_location_type_id FROM alpl_location_type_master where org_id=1")
    res_lt  = cursor.fetchall()
    loct_map=dict(res_lt)

def get_locations(cursor):
    global loc_map
    cursor.execute("SELECT alpl_reference_id,alpl_location_master_id FROM alpl_location_master where org_id=1")
    res_l  = cursor.fetchall()
    loc_map=dict(res_l)


def validate_string(val, val1):
    val=str(val)
    if val==None:
        return 'NULL'
    elif val1=='location_type_id':
        value = loct_map.get(int(val))
        if value!= None:
            return str(value)
        else:
            return 'NULL'
    elif val1=='from_location' or val1=='to_location':
        value = val #loc_map.get(int(val))
        if value!= None:
            if value.isnumeric():
                return str(loc_map.get(int(value),'NULL'))
            else:
                return "'"+value+"'"
        else:
            return 'NULL'
    
    elif type(val) in (int, float):
        if val != None and val != "":
            return val
        else:
            return 'NULL'
    elif val.replace('.', '', 1).isdigit() and val1 != 'int_indent_date':
        if val != None and val != "":
            return val
        else:
            return 'NULL'
    elif val1 == 'trip_creation_time' or val1 == 'trip_close_time':
        if val != None and val != "":
            return "'" + datetime.strptime(val, '%Y-%m-%d %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S') + "'"
        else:
            return 'NULL'
    elif val1 == 'bll_date' or val1 == 'dfd_created_date' or val1 == 'dc_date' or val1 == 'effective_from' or val1 == 'effective_to':
        if (val == '0000-00-00'):
            return 'NULL'
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
        if "'" in val:
            return "'" + val.replace("'","''") + "'"
        return "'" + val + "'"
    else:
        return 'NULL'

def validate_string_vendor_location(val,val1,org):
    val=str(val)
    if val1=='location_type_id' or val1=='from_location' or val1=='to_location':
        return loc_map.get(int(val))
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


def get_units(cursor):
    cursor.execute('SELECT alpl_unit_code,unit_id FROM alpl_unit_master')
    result_u = cursor.fetchall()
    dictUnitc = dict(result_u)
    return dictUnitc

def load_route_master(json,source,con):
    logger.info("Data ingestion starting for alpl_route_master")
    # json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    p = 0
    units= get_units(cursor)
    get_location_type(cursor)
    get_locations(cursor)
    ctu = 0
    cti = 0
    routeId = 0
    errors=0
    org_id=1
    cursor.execute('SELECT route_path_id,route_description FROM alpl_route_master where org_id=1')
    result = cursor.fetchall()
    pathList = [i[0] for i in result]
    
    if ("MINING" in source):
        columMap = {'route_path_id': 'RoutePathID', 'alpl_route_id': 'RouteID', 'from_location': 'Unit',
                    'to_location': 'District', 'route_description': 'RouteDescription', 'pit_id': 'PITID',
                    'bench_id': 'BenchID','org_id':''}
        org_id=2

    else:
        columMap = {'route_path_id': 'RoutePathID', 'alpl_route_id': 'RouteID', 'from_location': 'Unit',
                    'to_location': 'District', 'route_description': 'RouteDescription','unit_id':'UnitCode','org_id':''}

    p = 0

    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_route_master ('
        updateQuery = 'UPDATE alpl_route_master SET '
        i = 0
        flag = 0
        valueID = '0'
        q = None
        routeid=item.get('RouteID',None)
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'route_path_id' and (
                        int(item.get(json_column,None)) in pathList)):
                    if flag == 0:
                        value = item.get(json_column, None)
                        if(table_column=='org_id'):
                            value = org_id
                        if(table_column=='unit_id'):
                            value = units.get(int(value))
                        
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        valueID = validate_string(value, table_column)
                        flag = 1
                    if (i == 0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)
                        if(table_column=='org_id'):
                            value = org_id
                        if(table_column=='unit_id'):
                            value = units.get(int(value))
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)

                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
                        if(table_column=='org_id'):
                            value = org_id
                        if(table_column=='unit_id'):
                            value = units.get(int(value))
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
                        if(table_column=='org_id'):
                            value = org_id
                        if(table_column=='unit_id'):
                            value = units.get(int(value))
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        if(table_column=='unit_id'):
                            value = units.get(int(value))
                        if(table_column=='org_id'):
                            value = org_id
                        if (type(value) in (int, float)):
                            value = str(value)
                        if (value == None):
                            value = ""
                        dataset = dataset + ',' + validate_string(value, table_column)
                    i = 1
            dataset = dataset + ')'
            query = query +')'
            updateQuery = updateQuery + ' where route_path_id=' + valueID
            totalQuery = query + dataset
            if (flag == 1):
                cursor.execute(updateQuery)
                q = updateQuery
                ctu = ctu + 1
                # logger.info(q)
            else:
                cursor.execute(totalQuery)
                q = totalQuery
                cti = cti + 1
                # logger.info(q)
        except Exception as e:
            logger.info(str(e))
            logger.info(q)
            con.rollback()
            try:
                Util.insert_error_record('alpl_route_master', source, item, q, str(e),filename,routeid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            p = p + 1
            con.commit()

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_route_master:"+str( p))
    logger.info("connection closed successfully")
    logger.info("inserted :"+str(cti))
    logger.info("updated:"+str(ctu))


def load_leg_master(json,source,con):
    logger.info("Data ingestion starting for alpl_leg_master")
    cursor = con.cursor()
    get_location_type(cursor)
    get_locations(cursor)
    p = 0
    q = None
    org = 1
    src = 'logistics'
    errors = 0
    if ("MINING" in source):
        src = 'mining'
        org=2
    columMap = {'alpl_leg_id': 'id',
    'from_location': 'From_LocationID',
    'description': 'Description',
    'to_location': 'To_LocationID',
    'created_user_id' : 'CreatedUserID',
    'created_date_time': 'CreatedDateTime',
    'distance': 'distance',
    'target_time_in_mins': 'TargetTimeinMins'}
    listQuery="SELECT alpl_leg_id from alpl_leg_master where org_id="+str(org)
    cursor.execute(listQuery)
    result=cursor.fetchall()
    legIdList=[i[0] for i in result]
    for i, item in enumerate(json):
        dataset = 'values ('
        query = 'INSERT INTO alpl_leg_master ('
        updateQuery = 'UPDATE alpl_leg_master SET '
        i = 0
        flag = 0
        valueID = '0'
        legid=item.get('id',None)
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (table_column == 'alpl_leg_id' and (int(item.get(json_column,None)) in legIdList)):
                    if flag == 0:
                        value = item.get(json_column, None)
                        valueID = validate_string(value, table_column)
                        flag = 1
                    if(i==0):
                        updateQuery = updateQuery + table_column
                        value = item.get(json_column, None)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        value = item.get(json_column, None)
                        updateQuery = updateQuery + '=' + validate_string(value, table_column)
                    i = 1
                else:
                    if i == 0:
                        query = query + table_column
                        value = item.get(json_column, None)
                        dataset = dataset + validate_string(value, table_column)
                    else:
                        query = query + ',' + table_column
                        value = item.get(json_column, None)
                        dataset = dataset + ',' + validate_string(value, table_column)
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
                q=updateQuery
            else:
                cursor.execute(totalQuery)
                q=totalQuery
        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_leg_master', source, item, q, str(e),filename,legid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            p = p + 1
            con.commit()
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_leg_master:"+str(p))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")
    
def load_route_kmpl(json,source,con):
    logger.info("Data ingestion starting for alpl_route_kmpl")
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
        routepath=item.get('RoutePathID',None)
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
            cursor.execute(totalQuery)
        except Exception as e:
            logger.error(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_route_kmpl', source, item, q, str(e),filename,routepath)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            p = p + 1
            con.commit()
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_route_kmpl_master:"+str(p))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")


def load_state_master(json,source,con):
    logger.info("Data ingestion starting for alpl_state_master")
    #json_obj = getJsonFile(json,source)
    cursor = con.cursor()
    errors = 0
    src = 'logistics'
    if ("MINING" in source):
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
    stateIdQuery = 'select alpl_state_id from alpl_state_master'
    cursor.execute(stateIdQuery)
    result = cursor.fetchall()
    stateList = [i[0] for i in result]

    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_state_master ('
        updateQuery = 'UPDATE alpl_state_master SET '
        i = 0
        flag = 0
        valueID = None
        q = None
        stid=item.get('ALPLRefId',None)
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
                cursor.execute(updateQuery)
                uct = uct+1
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                ict = ict + 1
        except Exception as e:
            logger.info(str(e)+str(i))
            logger.info(q)
            logger.info(dataset)
            con.rollback()
            try:
                Util.insert_error_record('alpl_state_master', source, item, q, str(e),filename,stid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_state_master:"+str(p))
    logger.info("inserted :"+str(ict))
    logger.info("upated :"+str(uct))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")
    
def load_district_master(json,source,con):
    logger.info("Data ingestion starting for alpl_district_master")
    cursor = con.cursor()
    p = 0
    column_map = {
    'alpl_district_id': 'ALPLRefId',
    'district_name': 'Name',
    'district_code':'Code',
    'state_id' : 'StateId',
    #'rcl_district_code':'RCLDistCode'
         }
    uct = 0
    ict = 0
    errors = 0
    src = 'logistics'
    if ("MINING" in source):
        src = 'mining'
    districtIdQuery = 'select alpl_district_id from alpl_district_master'
    cursor.execute(districtIdQuery)
    result = cursor.fetchall()
    districtList = [i[0] for i in result]
    
    cursor.execute('SELECT alpl_state_id,state_master_id FROM alpl_state_master')
    result_s = cursor.fetchall()
    dictState = dict(result_s)

    for i, item in enumerate(json):

        dataset = ' values ('
        query = 'INSERT INTO alpl_district_master ('
        updateQuery = 'UPDATE alpl_district_master SET '
        i = 0
        flag = 0
        valueID = None
        q = None
        dstid=item.get('ALPLRefId',None)
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
                            value = str(dictState.get(int(value)))

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
                            value = str(dictState.get(int(value)))

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
                cursor.execute(updateQuery)
                uct = uct+1
            else:
                q = totalQuery
                cursor.execute(totalQuery)
                ict = ict + 1
        except Exception as e:
            logger.info(str(e)+str(i))
            logger.info(q)
            logger.info(dataset)
            con.rollback()
            try:
                Util.insert_error_record('alpl_district_master', source, item, q, str(e),filename,dstid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p = p + 1

    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_district_master:"+str( p))
    logger.info("inserted :"+str(ict))
    logger.info("upated :"+str(uct))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")
    
def load_location_master(jsonData,source,con):
    logger.info("Data ingestion starting for alpl_location_master")
    cursor = con.cursor()
    get_location_type(cursor)
    src = 'logistics'
    errors = 0
    org = 1
    q=None
    if ("MINING" in source):
        src = 'mining'
        org = 2
    p = 0
    columMap = {'alpl_reference_id': 'ALPLRefId',
                 'location_name': 'Name',
                  'location_code': 'LocationCode',
                'location_type_id': 'LocationTypeId',
                 'rcl_cust_id': 'RCLCustId',
                  'address_line1': 'AddressLine1',
                'address_line2': 'AddressLine2',
                 'city_id': 'CityID',
                  'district_id': 'DistrictID',
                'state_id': 'StateID',
                 'country_id': 'CountryID',
                  'pincode': 'Pincode',
                   'contact_no': 'ContactNo',
                'gst_no': 'GSTNo',
                 'created_user_id': 'CreatedUserId',
                  'created_date_time': 'CreatedDateTime',
                  'lat':'Latitude',
                'lon':'Longitude'}

    listQuery = "SELECT alpl_reference_id from alpl_location_master where org_id=" + str(org)
    cursor.execute(listQuery)
    result = cursor.fetchall()
    locationIdList = [i[0] for i in result]

    for i, item in enumerate(jsonData):
        dataset='values ('
        insertQuery='INSERT INTO alpl_location_master ('
        updateQuery='UPDATE alpl_location_master SET '
        j=0
        flag=0
        value='0'
        lcid=item.get('ALPLRefId',None)
        try:
            for table_column, json_column in columMap.items():
                if (flag == 1) or (
                        table_column == 'alpl_reference_id' and (int(item.get(json_column, None)) in locationIdList)):
                    if flag == 0:
                        value = validate_string(item.get(json_column, None), table_column)
                    flag = 1
                    if j == 0:
                        updateQuery = updateQuery + table_column
                        updateQuery = updateQuery + '=' + validate_string(item.get(json_column, None),table_column)
                    else:
                        updateQuery = updateQuery + ',' + table_column
                        updateQuery = updateQuery + '=' + validate_string(item.get(json_column, None),table_column)

                    j = 1
                else:
                    if j == 0:
                        insertQuery = insertQuery + table_column
                        dataset = dataset + validate_string(item.get(json_column, None), table_column)
                    else:
                        insertQuery = insertQuery + ',' + table_column
                        dataset = dataset + ',' + validate_string(item.get(json_column, None),table_column)

                    j = 1
            insertQuery = insertQuery + ',' + "org_id"
            dataset = dataset + ',' + str(org)
            dataset = dataset + ')'
            insertQuery = insertQuery + ')'
            updateQuery = updateQuery + ' where alpl_reference_id=' + value
            updateQuery = updateQuery + ' and org_id=' + str(org)
            totalQuery = insertQuery + dataset
            q = totalQuery
            if flag:
                cursor.execute(updateQuery)
            else:
                cursor.execute(totalQuery)

        except Exception as e:
            logger.info(str(e))
            logger.info(q)
            con.rollback()

            try:
                Util.insert_error_record('alpl_location_master', source, item, q, str(e),filename,lcid)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1

        else:
            con.commit()
            p = p + 1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_location_master:"+str( p))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")

def load_location_type_master(jsonData, source,con):
    logger.info("Data ingestion starting for alpl_location_type_master")
    cursor = con.cursor()
    p = 0
    errors = 0
    src = 'logistics'
    org =1
    q=None
    if ("MINING" in source):
        src = 'mining'
        org=2
    columMap={'alpl_reference_id':'ALPLRefId','location_name':'Name','location_type':'LocationType'}
    listQuery="SELECT alpl_reference_id from alpl_location_type_master where org_id="+str(org)
    cursor.execute(listQuery)
    result=cursor.fetchall()
    locationTypeIdList=[i[0] for i in result]
    for i, item in enumerate(jsonData):
        dataset=' values ('
        insertQuery='INSERT INTO alpl_location_type_master ('
        updateQuery='UPDATE alpl_location_type_master SET '
        i=0
        flag=0
        value='0'
        lctype=item.get('ALPLRefId',None)
        try:
            for table_column,json_column in columMap.items():
                if (flag==1) or (table_column=='alpl_reference_id' and (int(item.get(json_column,None)) in locationTypeIdList)):
                    if flag==0:
                        value=validate_string(item.get(json_column,None),table_column)
                    flag=1
                    if i==0:
                        updateQuery=updateQuery+table_column
                        updateQuery=updateQuery+'='+validate_string(item.get(json_column,None),table_column)
                    else:
                        updateQuery=updateQuery+','+table_column
                        updateQuery=updateQuery+'='+validate_string(item.get(json_column,None),table_column)

                    i=1
                else:
                    if i==0:
                        insertQuery=insertQuery+table_column
                        dataset=dataset+validate_string(item.get(json_column,None),table_column)
                    else:
                        insertQuery=insertQuery+','+table_column
                        dataset=dataset+','+validate_string(item.get(json_column,None),table_column)

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
            else:
                cursor.execute(totalQuery)

        except Exception as e:
            logger.info(str(e))
            con.rollback()
            try:
                Util.insert_error_record('alpl_location_type_master', source, item, q, str(e),filename,lctype)
            except Exception as exp:
                logger.error(str(exp))
            else:
                con.commit()
                errors = errors + 1
        else:
            con.commit()
            p=p+1
    con.close()
    logger.info("Data ingestion completed. No of rows processed for alpl_location_type_master:"+str(p))
    logger.info("errors :"+str(errors))
    logger.info("connection closed successfully")


def main(file_name):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #file = event
    #file = os.path.abspath('E:/prod_dump') + "/0803/route.json"

    try:
        #json_data = open(file).read()
        #json_obj = json.loads(json_data)
        global filename
        filename=file_name
        json_obj = Util.getJsonFile(Util.portal_bucket, file_name)
        #logger.info(f"Processing file: {file['name']}.")
        logger.info("Processing file: "+file_name)
        data = str(json_obj)
        ch = data.find('[')
        dataKey = data[ch-7:ch-3]
        sourcetype='PORTAL'
        data = data[2:8]
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
        if source=='ROUTE_MASTER' :
            func = load_route_master
        elif source=='ROUTELEG_MASTER':
            pass
        elif source=='ROUTEKMPL_MASTER':
            pass
            func = load_route_kmpl
        elif source=='LEG_MASTER':
            pass
            func = load_leg_master
        elif source=='LOCATIONTYPE_MASTER':
            pass
            func =  load_location_type_master
        elif source=='LOCATION_MASTER':
            pass
            func = load_location_master
        elif source=='LEG_TIME_MASTER':
            pass
        elif source=='DISTRICT_MASTER' :
            pass
            func = load_district_master
        elif source=='STATE_MASTER':
            pass
            func = load_state_master
        if func != None:
            MultiProc.runDataIngestion(jsonData, func, source)
        Util.move_blob(Util.portal_bucket, file_name, dest_bucket, file_name)
        Util.call_error_data_url(filename, sourcetype)
    except Exception as ex:
        logger.info("Halting json data load processing due to following exception:" + str(ex))
        Util.move_blob(Util.portal_bucket, file_name, Util.error_bucket, file_name)
        Util.send_alert(file_name, str(ex))