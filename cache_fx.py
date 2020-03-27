## Cache's function file

import requests
import urllib.parse as parse
import pandas as pd
import numpy as np
import os.path
import math
from io import StringIO
import sys
import pickle
import sqlalchemy as db
from time import time
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

def sis_auth_chk():
    creds = 0
    if os.path.isfile('sis_config.pkl'):
        print('Config info exists')
        token_chk()
        creds = pickle.load(open('sis_config.pkl','rb'))
    else:
        print('NO CONFIG INFO PRESENT!')
        creds = build_sis_config2()
    return creds

def sql_auth_chk():
    creds = 0
    if os.path.isfile('sql_config.pkl'):
        print('SQL config info exists')
        creds = pickle.load(open('sql_config.pkl','rb'))
    else:
        print('NO SQL CONFIG INFO PRESENT!')
        creds = build_sql_config2()
    return creds

def build_sis_config(username,password,endpoint='https://www.agstrata.net/'):
    temp = {'username':username,'password':password,'endpoint':endpoint}
    ret_token(uname=username,pword=password,endPoint=endpoint)
    pickle.dump(temp,open('sis_config.pkl','wb'))
    print('Sisense config file successfully built')
    return

def build_sis_config2():
    username = input('Username: ')
    password = input('Password: ')
    print('To use the default endpoint, enter 0')
    endpoint = input('Endpoint: ')
    if endpoint == str(0):
        endpoint = 'https://www.agstrata.net/'
    temp = {'username':username,'password':password,'endpoint':endpoint}
    ret_token(uname=username,pword=password,endPoint=endpoint)
    pickle.dump(temp,open('sis_config.pkl','wb'))
    print('Sisense config file successfully built')
    return temp

def build_sql_config(username,password,address='172.16.15.3',database='astrata1'):
    temp = {'username':username,'password':password,'address':address,
            'database':database}
    pickle.dump(temp,open('sql_config.pkl','wb'))
    print('SQL config file successfully built')
    return

def build_sql_config2():
    username = input('Username: ')
    password = input('Password: ')
    print('To use the default address, enter 0')
    address = input('Address: ')
    print('To use the default database, enter 0')
    database = input('Database: ')
    if address == str(0) and database == str(0):
        temp = {'username':username,'password':password,'address':'172.16.15.3',
                'database':'astrata1'}
    elif address == str(0):
        temp = {'username':username,'password':password,'address':'172.16.15.3',
                'database':database}
    elif database == str(0):
        temp = {'username':username,'password':password,'address':address,
                'database':'astrata1'}
    else:
        temp = {'username':username,'password':password,'address':address,
                'database':database}
    pickle.dump(temp,open('sql_config.pkl','wb'))
    print('SQL config file successfully built')
    return temp

def ret_token(uname,pword,endPoint='https://www.agstrata.net/'):
    ## Non-parsed username and password
    ## EndPoint preset to AgStrata, not a required variable
    if endPoint[-1] == "/":
        endPoint = endPoint + 'api/v1/authentication/login'
    else:
        endPoint = endPoint + '/api/v1/authentication/login'
    header = {parse.quote('accept'): parse.quote('application/json'),
              parse.quote('content-type'): parse.quote('application/x-www-form-urlencoded')}
    body = {'username': uname,
            'password': pword}

    response = requests.post(endPoint, headers=header, data=body)

    if response.ok:
        data = response.text
        xx = data.find("access_token")
        yy = xx + 15
        new_str = data[yy:]
        xx = new_str.find(",")
        token = new_str[:xx-1]
        print('Retrieved authentication token from Sisense')
        tok_time = time()
        temp_var = {'token':token,'token_time':tok_time}
        pickle.dump(temp_var,open('sis_token.pkl','wb'))
    else:
        token = " "
        print('Invalid login credentials. Please check.')
        build_sis_config2()
    return token

def token_chk():
    creds = 0
    if os.path.isfile('sis_token.pkl'):
        print('Sisense token info exists')
        creds = pickle.load(open('sis_token.pkl','rb'))
        config = pickle.load(open('sis_config.pkl','rb'))
        cur_time = time()
        if creds['token_time']+86400 < cur_time:
            print('Please generate a new token')
            ret_token(config['username'],config['password'],endPoint=config['endpoint'])
    else:
        print('NO SISENSE TOKEN INFO PRESENT!')
        ret_token(config['username'],config['password'],endPoint=config['endpoint'])
    return creds

def query_elast(dataSource,query,token,endPoint='https://www.agstrata.net/'):
    ## Non-parsed dataSource, query, and token
    ## EndPoint preset to AgStrata, not a required variable
    if endPoint[-1] == "/":
        endPoint = endPoint + 'api/datasources'
    else:
        endPoint = endPoint + '/api/datasources'
    dataSourceURI = parse.quote(dataSource)
    queryRUI = parse.quote(query)

    endPoint = endPoint + '/{}/Sql?format=csv&query={}'.format(dataSourceURI, queryRUI)
    header = {'Authorization' : 'Bearer '+token}

    print('Querying Elasticube...')
    response = requests.get(endPoint, headers=header)
    data = response.text

    df = pd.read_csv(StringIO(data))
    print('Data retrieved')
    return df
    
def build_elast(token,ecube,endPoint='https://www.agstrata.net/'):
    if endPoint[-1] == "/":
        endPoint = endPoint + 'api/elasticubes/localhost'
    else:
        endPoint = endPoint + '/api/elasticubes/localhost'
    dataSourceURI = parse.quote(ecube)
    endPoint = endPoint + '/{}/startBuild?type=full'.format(dataSourceURI)
    header = {'Authorization' : 'Bearer '+token}
    print('Building Elasticube {}'.format(ecube))
    response = requests.post(endPoint, headers=header)
##    data = response.text
##    df = pd.read_csv(StringIO(data))
    print('Data retrieved')
    return

def geo_code(token,city,state,country='United States',place="",endPoint='https://www.agstrata.net/',level="city"):
    print('Geo-coding location:')
    print(place,city,',',state,',',country)
    endPoint = endPoint + 'api/v1/geo/locations'
    header = {parse.quote('accept'): parse.quote('application/json'),
              parse.quote('content-type'): parse.quote('application/json'),
              parse.quote('Authorization'): 'Bearer '+token}
    if place != "":
        body = {
                'locations': [
                    {
                        'name': str(place),
                        'country': str(country),
                        'state': str(state),
                        'city': str(city)
                    }
                    ],
                'geoLevel': level
                }
        response = requests.post(endPoint, headers=header, json=body)
        data = response.text
    else:
        body = {
                'locations': [
                    {
                        'name': str(city),
                        'country': str(country),
                        'state': str(state)
                    }
                    ],
                'geoLevel': level
                }
        response = requests.post(endPoint, headers=header, json=body)
        data = response.text
    if data.find('latLng') < 0:
        print('Error occured. See request text.')
        lat = 0
        long = 0
    else:
        nn = data.find('latLng') + 15
        temp_data = data[nn:]
        mm = temp_data.find('lng')
        lat = float(temp_data[:mm-2])
        print('Latitude: ',lat)
        temp_data2 = temp_data[mm+5:]
        nn = temp_data2.find('context') - 3
        long = float(temp_data2[:nn])
        print('Longitude: ',long)
    output = [lat, long]
    return output

def haversine_dist(latlon1, latlon2, r=3958.8):
    lat1 = latlon1[0]
    lon1 = latlon1[1]
    lat2 = latlon2[0]
    lon2 = latlon2[1]
    dist = 2 * r * math.asin(math.sqrt(math.sin(math.radians((lat2-lat1)/2))**2+math.cos(math.radians(lat1))*
                                        math.cos(math.radians(lat2))*math.sin((math.radians(lon2-lon1)/2))**2))
    return dist

def get_dist(token, city1, state1, city2, state2, country="United States",endPoint='https://www.agstrata.net/',level="city"):
    latlon1 = geo_code(token, city1, state1)
    latlon2 = geo_code(token, city2, state2)
    dist = int(haversine_dist(latlon1, latlon2))
    return dist

def conn_db(uname,pword,table,address='172.16.15.3',database='astrata1'):
    conn_str = ('mssql+pymssql://' + uname + ':' + pword + '@' + address +
                '\\' + database + '/' + table)
    engine = db.create_engine(conn_str)
    return engine

def query_db(query,engine):
    try:
        cnxn = engine.connect()
        print('Database connected')
    except:
        print('Database could not be connected')
        output_data = pd.DataFrame(['Error'])
        return output_data
    output_data = pd.read_sql_query(query,cnxn)
    return output_data

def sql_query(query,uname,pword,table,address='172.16.15.3',database='astrata1'):
    engine = conn_db(uname,pword,table,address,database)
    print('Engine created')
    data = query_db(query,engine)
    print('Query executed')
    return data

def build_model(mod_data,cnxn):
    model_data = mod_data
    print("Building model")
    fdydid = list(set(model_data["feedyardid"]))
    types = list(set(model_data["cattletypeid"]))
    months = list(set(model_data["monthin"]))

    typ_no = len(types)
    mon_no = len(months)

    skip_yds = []

    #del results

    for xx in fdydid:
        print('Yard:',xx)
        X = pd.DataFrame([model_data[model_data["feedyardid"]==xx]["InWt"],model_data[model_data["feedyardid"]==xx]["cattletypeid"],
                         model_data[model_data["feedyardid"]==xx]["monthin"]]).T
        y = pd.DataFrame(model_data[model_data["feedyardid"]==xx]["DOF"])
        temp1 = np.array(X['InWt'])[np.newaxis].T
    #    print(temp1.shape)
        int_no = len(temp1)
        if int_no > 500:
            temp2 = np.zeros((int_no,typ_no))
            temp3 = np.zeros((int_no,mon_no))
            for yy in range(int_no):
                for zz in range(typ_no):
                    if X['cattletypeid'].iloc[yy] == types[zz]:
                        temp2[yy,zz] = 1
                for zz in range(mon_no):
                    if X['monthin'].iloc[yy] == months[zz]:
                        temp3[yy,zz] = 1
    #    print(temp2.shape)
    #    print(temp3.shape)
            xtrain = np.hstack((temp1,temp2,temp3))
    #    print(temp4.shape)
            regr = linear_model.LinearRegression()
            regr.fit(xtrain,y)
            ynew = regr.predict(xtrain)
            print("Model score: %.2f" % regr.score(xtrain,y))
            bb = regr.coef_
            intc = regr.intercept_
        # The coefficients
            print('Coefficients: \n', bb)
        # The mean squared error
            MSE = mean_squared_error(y, ynew)
            print("Mean squared error: %.2f"
                    % MSE)
        # Explained variance score: 1 is perfect prediction
            r2 = r2_score(y, ynew)
            print('Variance score: %.2f' % r2)
            inter_var = [xx, bb[0][0],bb[0][1],bb[0][2],bb[0][3],bb[0][4],bb[0][5],
                         bb[0][6],bb[0][7],bb[0][8],bb[0][9],bb[0][10],bb[0][11],
                         bb[0][12],bb[0][13],bb[0][14],bb[0][15],bb[0][16], MSE, r2, 
                         float(intc)]
            try:
                results = np.vstack((results, np.array(inter_var)[np.newaxis]))
            except NameError:
                results = np.array(inter_var)[np.newaxis]
            results2 = pd.DataFrame(results,columns=['FDYDID','InWt','CT1','CT2','CT3',
                                                   'CT7','JAN','FEB','MAR','APR','MAY',
                                                   'JUN','JUL','AUG','SEP','OCT','NOV',
                                                   'DEC','MSE','R2','INTERCEPT'])
            print(" ")
        else:
            print(" ")
            print('Yard',xx,'skipped')
            skip_yds = np.append(skip_yds,xx)
            print(" ")

    yds = list(set(results2['FDYDID']))
    nn = len(yds)

    #results3 = np.zeros(nn*4*12,4)

    temp_arr = np.array([0,0,0,0,0])[np.newaxis]

    for xx in range(nn):
    #    print(xx)
        inwt = results2['InWt'][xx]
        cts = results2.iloc[xx,2:5]
        mos = results2.iloc[xx,6:-3]
        intc = results2.iloc[xx,-1]
        for yy in range(len(cts)):
            for zz in range(len(mos)):
                temp = np.array([yds[xx]] + [yy+1] + [zz+1] + [inwt] + [cts[yy] + mos[zz] + intc])[np.newaxis]
                temp_arr = np.vstack((temp_arr,temp))

    results3 = pd.DataFrame(temp_arr[1:,:],columns=['FDYDID','CATTLETYPEID','MONTHIN','INWT','INTERCEPT'])
    print('Finished')

    results3.to_sql("DOF_Regression_Results_Flattened",cnxn,if_exists='replace')
    print('Results added to SQL Database')
    return skip_yds
