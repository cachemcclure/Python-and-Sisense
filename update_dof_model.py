import sqlalchemy as db
import pandas as pd
##from sklearn.preprocessing import OneHotEncoder, StandardScaler
##from sklearn.compose import ColumnTransformer
##from sklearn.pipeline import Pipeline
##from sklearn.impute import SimpleImputer
##import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
##from sklearn.svm import SVR

##engine = db.create_engine('mssql+pymssql://shawn:stw!2027@sql\\astrata1/cattletrack')
engine = db.create_engine('mssql+pymssql://shawn:stw!2027@172.16.15.3\\astrata1/cattletrack')
cnxn = engine.connect()
print("Database connected")

print("Beginning query")
query = '''Select g.groupid, closeouts.feedyardid,cattletypeid,
[DryPounds]/([PayWeightOut]-[PayWeightIn]) as DMCONV ,[PayWeightIn]/[HeadsIn] as InWt, [DaysOnFeed]/headsin  as DOF, 
PayWeightOut/headsin  as Outwt, closeoutdate, (dateadd(dd,(([DaysOnFeed]/headsin)*-1),CloseoutDate)) indate,
month(dateadd(dd,(([DaysOnFeed]/headsin)*-1),CloseoutDate)) monthin
from closeouts
inner join RelFeedYardsGroups g on g.feedyardid = closeouts.feedyardid
where [DryPounds]/([PayWeightOut]-[PayWeightIn]) between 3 and 12 and PayWeightIn/headsin between 400 and 999 
and groupid in (5,7,9,12,13,10) and cattletypeid in (1,2,3,7)'''
model_data = pd.read_sql_query(query,cnxn)
##print(model_data.shape)
##model_data.head(10)

print("Query complete")

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
        skip_yds = skip_yds + [xx]
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
