## Build Sisense Elasticube
##
## Add the username/email and password for your implementation of Sisense
## in between the apostrophes (').
## Change the endpoint to match your implementation, then run. The output
## should be an empty dataframe.

from cache_fx import ret_token
from cache_fx import build_elast

username = ''
password = ''
ecube = ''
endpoint = 'https://www.agstrata.net/'

token = ret_token(username,password,endPoint=endpoint)
output = build_elast(token,ecube,endPoint=endpoint)

print(output)
