from configparser import ConfigParser
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
class DBInterface():
    
    def __init__(self,table):
        table = self.table
        
    cfg = ConfigParser()
    cfg.read(r'../config.ini')
    DB_CONFIG = {
    'database':cfg.get('BBDD_CONNECTION','ddbb'),
    'user':cfg.get('BBDD_CONNECTION','user'),
    'password':cfg.get('BBDD_CONNECTION','password'),
    'host':cfg.get('BBDD_CONNECTION','host'),
    'port': cfg.get('BBDD_CONNECTION','port')}
    
    global DB_CONFIG_URL
    DB_CONFIG_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@localhost:{DB_CONFIG['port']}/{DB_CONFIG['database']}')"
    
    global connect
    def connect():
        
        return psycopg2.connect(DB_CONFIG_URL)
        #host=DB_CONFIG["host"],
        #database=DB_CONFIG["database"],
        #user=DB_CONFIG["user"],
        #password=DB_CONFIG["password"])
    
    
    def get_schema(table: str):
        connection = connect()
        sql = """SELECT * FROM information_schema.columns WHERE table_name = '{table}'"""
        schema = sqlio.read_sql_query(sql, connection)
        connection.close()
        return schema

    def upload_data(data: pd.DataFrame):
        
        
        #cursor = connection.cursor()
        #cursor.execute(sql)
        #conn.commit()
        #cursor.close()
        #conn.close()
    
 

#conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, username, pwd))
#sql = "select count(*) from table;"
#dat = sqlio.read_sql_query(sql, conn)
#conn = None

#from pandas import DataFrame

def upload_data(data: DataFrame, table: str, how = 'append'):
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:gorkamorka@localhost:5432/walknet')
    data.to_sql(table, engine, if_exists = how, index=False)



#TODO
#def reboot_schema() FOR INSTALLATION 
#def basic_statistics() FOR DATABASE ELEMENTS CONTROL

#get_schema()

#copy_upload()

#execute_query()