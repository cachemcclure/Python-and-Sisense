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

def sis_auth_chk():
    creds = 0
    if os.path.isfile('sis_config.pkl'):
        print('Config info exists')
        creds = pickle.load(open('sis_config.pkl','rb'))
    else:
        print('NO CONFIG INFO PRESENT!')
        sys.exit()
    return creds

def sql_auth_chk():
    creds = 0
    if os.path.isfile('sql_config.pkl'):
        print('SQL config info exists')
        creds = pickle.load(open('sql_config.pkl','rb'))
    else:
        print('NO SQL CONFIG INFO PRESENT!')
        sys.exit()
    return creds

def build_sis_config(username,password,endpoint='https://www.agstrata.net/'):
    temp = {'username':username,'password':password,'endpoint':endpoint}
    ret_token(uname=username,pword=password,endPoint=endpoint)
    pickle.dump(temp,open('sis_config.pkl','wb'))
    print('Sisense config file successfully built')
    return

def build_sql_config(username,password,address='172.16.15.3',database='astrata1'):
    temp = {'username':username,'password':password,'address':address,
            'database':database}
    pickle.dump(temp,open('sql_config.pkl','wb'))
    print('SQL config file successfully built')
    return

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
        sys.exit()
    return token

def token_chk():
    creds = 0
    if os.path.isfile('sis_token.pkl'):
        print('Sisense token info exists')
        creds = pickle.load(open('sis_token.pkl','rb'))
        cur_time = time()
        if creds['token_time']+86400 < cur_time:
            print('Please generate a new token')
            sys.exit()
    else:
        print('NO SISENSE TOKEN INFO PRESENT!')
        sys.exit()
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
    data = response.text

    df = pd.read_csv(StringIO(data))
    print('Data retrieved')
    return df

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
        sys.exit()
    output_data = pd.read_sql_query(query,cnxn)
    return output_data

def sql_query(query,uname,pword,table,address='172.16.15.3',database='astrata1'):
    engine = conn_db(uname,pword,table,address,database)
    print('Engine created')
    data = query_db(query,engine)
    print('Query executed')
    return data
