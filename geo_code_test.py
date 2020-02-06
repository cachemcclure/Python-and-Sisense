## Build Sisense Elasticube
##
## Add the username/email and password for your implementation of Sisense
## in between the apostrophes (').
## Change the endpoint to match your implementation, then run. The output
## will print in the command/developer window.

from cache_fx import ret_token
from cache_fx import get_dist
from cache_fx import auth_chk

config = auth_chk()

username = config['username']
password = config['password']
endpoint = config['endpoint']

city = 'Amarillo'
state = 'Texas'

token = ret_token(username,password)

city2 = 'Weatherford'
state2 = 'Oklahoma'

yy = get_dist(token, city, state, city2, state2, endPoint=endpoint)

print('The distance between ' + city + ', ' + state + ' and ' + city2 + ', ' +
      state2 + ' is ' + str(yy) + ' miles.')
