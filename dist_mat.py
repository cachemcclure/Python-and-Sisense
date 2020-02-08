## Haversine Distance Matrix
##
## Builds haversine distance matrix between all unique pairs of feedyard/
## auction.

from cache_fx import sis_auth_chk
from cache_fx import sql_auth_chk
from cache_fx import ret_token
from cache_fx import get_dist
from cache_fx import sql_query
import numpy as np

config = sis_auth_chk()

username = config['username']
password = config['password']
endpoint = config['endpoint']

token = ret_token(username,password,endPoint=endpoint)

sql_config = sql_auth_chk()

sqlusername = sql_config['username']
sqlpassword = sql_config['password']
sqladdress = sql_config['address']
sqldatabase = sql_config['database']

table = 'beefspotter'

query = '''select feedyardname, city, state from feedlotmaster'''

yard_data = sql_query(query,sqlusername,sqlpassword,table,address=sqladdress,
                      database=sqldatabase)

query = '''select slugname, city, state from barnsusda'''

auct_data = sql_query(query,sqlusername,sqlpassword,table,address=sqladdress,
                      database=sqldatabase)

aa, bb = yard_data.shape
cc, dd = auct_data.shape

for xx in range(aa):
    temp_line = []
    for yy in range(cc):
        temp = get_dist(token,yard_data['city'][xx],yard_data['state'][xx],
                        auct_data['city'][yy],auct_data['state'][yy])
        temp_line = np.append(temp_line,temp)
        print('Distance from '+yard_data['city'][xx]+', '+yard_data['state'][xx]+
              ' to '+auct_data['city'][yy]+', '+auct_data['state'][yy]+' is '+
              str(temp)+' miles.')
    try:
        dist_mat = np.vstack((dist_mat,temp_line[np.newaxis].T))
    except NameError:
        dist_mat = temp_line[np.newaxis].T

print('Finished')
