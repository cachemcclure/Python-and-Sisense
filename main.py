## Top Menu Function
from cache_fx import build_sis_config
from cache_fx import build_sql_config
from cache_fx import sis_auth_chk
from cache_fx import ret_token
from cache_fx import token_chk
from cache_fx import query_elast
from cache_fx import geo_code
from cache_fx import get_dist
from cache_fx import sql_query
from cache_fx import build_elast
import pandas as pd
import sys

def zero():
    print('help statement here')
    return main()

def one():
    uname = input('Sisense username: ')
    pword = input('Sisense password: ')
    print('Enter 0 to use the default Sisense address')
    ep = input('Sisense root web adddress: ')
    if ep == 0:
        build_sis_config(uname,pword)
    else:
        build_sis_config(uname,pword,endpoint=ep)
    print(' ')
    input('Press enter to return to the main menu')
    print(' ')
    return main()

def two():
    uname = input('SQL username: ')
    pword = input('SQL password: ')
    print('Enter 0 to use the default SQL address')
    adre = input('SQL address: ')
    print('Enter 0 to use the default SQL database')
    dbn = input('SQL database name: ')
    if adre == 0 and dbn == 0:
        build_sql_config(uname,pword)
    elif adre == 0:
        build_sql_config(uname,pword,database=dbn)
    elif dbn == 0:
        build_sql_config(uname,pword,database=dbn)
    else:
        build_sql_config(uname,pword,address=adre,database=dbn)
    print(' ')
    input('Press enter to return to the main menu')
    print(' ')
    return main()

def three():
    config = sis_auth_chk()
    token = ret_token(config['username'],config['password'],endPoint=config['endpoint'])
    print(token)
    print(' ')
    input('Press enter to return to the main menu')
    print(' ')
    return main()

def four():
    config = token_chk()
    token = config['token']
    config2 = sis_auth_chk()
    endpoint = config2['endpoint']
    datasource = input('Elasticube name: ')
    query = input('Query: ')
    save_nm = input('Please enter a name for the output file: ')
    output = query_elast(datasource,query,token,endPoint=endpoint)
    output.to_csv(save_nm+'.csv')
    print(' ')
    input('Press enter to return to the main menu')
    print(' ')
    return main()

def five():
    config = token_chk()
    token = config['token']
    config2 = sis_auth_chk()
    endpoint = config2['endpoint']
    city = input('City: ')
    state = input('State: ')
    out = geo_code(token,city,state)
    print(' ')
    input('Press enter to return to the main menu')
    print(' ')
    return main()

def six():
    config = token_chk()
    token = config['token']
    config2 = sis_auth_chk()
    endpoint = config2['endpoint']
    city1 = input('First city: ')
    state1 = input('First state: ')
    city2 = input('Second city: ')
    state2 = input('Second state: ')
    out = get_dist(token,city1,state1,city2,state2,endPoint=endpoint)
    print(' ')
    input('Press enter to return to the main menu')
    print(' ')
    return main()

def seven():
    config = sql_auth_chk()
    username = config['username']
    password = config['password']
    address = config['address']
    database = config['database']
    table = input('Table name: ')
    query = input('Query: ')
    save_nm = input('Please enter a name for the output file: ')
    out = sql_query(query,username,password,table,address=address,database=database)
    out.to_csv(save_nm+'.csv')
    print(' ')
    input('Press enter to return to the main menu')
    print(' ')
    return main()

def eight():
    config = token_chk()
    token = config['token']
    config2 = sis_auth_chk()
    endpoint = config2['endpoint']
    ecube = input('Elasticube: ')
    out = build_elast(token,ecube,endPoint=endpoint)
    print('Elasticube building')
    print(' ')
    input('Press enter to return to the main menu')
    print(' ')
    return main()

def switch_fx(argument):
    switcher = {
        1: one,
        2: two,
        3: three,
        4: four,
        5: five,
        6: six,
        7: seven,
        8: eight,
        9: sys.exit
        }
    func = switcher.get(argument, lambda: "Invalid entry")
    return func()

def main():
    print("WELCOME TO THE TOP MENU")
    print(' ')
    print('MENU')
    print('0 - Help')
    print('1 - Build Sisense Configuration file')
    print('2 - Build SQL Configuration file')
    print('3 - Retrieve Sisense token')
    print('4 - Query Sisense Elasticube')
    print('5 - Geo-code a location')
    print('6 - Find distance between two locations')
    print('7 - Query SQL database')
    print('8 - Build Elasticube')
    print('9 - Exit')

    var = input('Choose Option: ')
    switch_fx(int(var))
    return

main()
