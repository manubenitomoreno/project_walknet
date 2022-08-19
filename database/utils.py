from pandas import DataFrame

def upload_data(data: DataFrame, table: str, how = 'append'):
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:gorkamorka@localhost:5432/walknet')
    data.to_sql(table, engine, if_exists = how, index=False)
    
#TODO
#def reboot_schema() FOR INSTALLATION 
#def basic_statistics() FOR DATABASE ELEMENTS CONTROL