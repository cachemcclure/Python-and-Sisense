## Top Menu Function
from cache_fx import build_sis_config
from cache_fx import build_sql_config
from cache_fx import sis_auth_chk

def zero():
    print('help statement here')
    return

def one():
    uname = input('Sisense username: ')
    pword = input('Sisense password: ')
    print('Enter 0 to use the default Sisense address')
    ep = input('Sisense root web adddress: ')
    if ep == 0:
        build_sis_config(uname,pword)
    else:
        build_sis_config(uname,pword,endpoint=ep)
    return

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
    return

def three():
    config = sis_auth_chk()
    token = ret_token(config[0],config[1],endPoint=config[2])
    print(token)
    return

def switch_fx(argument):
    switcher = {}
    return

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
print('9 - Exit')

var = input('Choose Option')
