## Sisense Elasticube SQL Query
##
## Add the username/email and password for your implementation of Sisense
## in between the apostrophes (').
## Change the endpoint to match your implementation, then run. The output
## should be a CSV file containing the results of your query. It will be
## saved in the parent directory of this script.

from cache_fx import ret_token
from cache_fx import query_elast
from cache_fx import auth_chk

save_nm = 'output.csv'

config = auth_chk()

username = config['username']
password = config['password']
endpoint = config['endpoint']

dataSource = 'DailyFYData'

query = '''SELECT FeedyardName, Lot, Pen, Sex, HeadPlaced, TotalHeadShipped,
        CurrentHeadCount, DateClosed,
        ProjectedOutDate, AgStrata_Proj_OutDate2 from LotPenSummary_Updated
        where (DateClosed between #10/31/2019# and #12/1/2019#) or
        (ProjectedOutDate between #10/31/2019# and #12/1/2019#) or
        (AgStrata_Proj_OutDate2 between #10/31/2019# and #12/1/2019#)'''

token = ret_token(username,password,endPoint=endpoint)

data = query_elast(dataSource,query,token,endPoint=endpoint)

data.to_csv(save_nm)
