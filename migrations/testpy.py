import pandas as pd
from snowflake.connector import connect
import os

import pandas.io.sql as psql
connection = connect(
        user='shristi.suyog',
        password='C0llaborate!!**19',
        account='UJ75171',
        warehouse = 'LOAD_WH',
        database = 'DATALAKE',
        schema = 'PUBLIC'
    )
cur = connection.cursor()
databases = ['DATALAKE']
parent_dir = r'C:\Users\Sunit_Tanwar.MIPL\Downloads\Snow_Git'
for i in databases:
    db_directory = i
    path = os.path.join(parent_dir, db_directory) 
    os.mkdir(path)
    
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
    for j in df['name']:
        sc_directory = j
        path = os.path.join(parent_dir, db_directory,sc_directory) 
        os.mkdir(path)
        tablespath = os.path.join(parent_dir, db_directory,sc_directory,'Tables')
        viewspath = os.path.join(parent_dir, db_directory,sc_directory,'Views')
        procedurespath = os.path.join(parent_dir, db_directory,sc_directory,'Procedures')
        
        
        os.mkdir(viewspath)
        os.mkdir(procedurespath)
        db_schema=i +"." + j
        print (db_schema)
        tables_sql = cur.execute(f"show tables in {db_schema}")
        print(tables_sql)
        if cur.rowcount!=0:
            os.mkdir(tablespath)
        else:
            g=0
        if cur.rowcount!=0:
             tables_df = pd.DataFrame(cur.fetchall())
             tables_df.columns = [column[0] for column in cur.description]
             for k in tables_df['name']:
                    path = os.path.join(parent_dir, db_directory,sc_directory,'Tables',k)
                    os.mkdir(path)
                    db_schema_table=i+"."+j+"."+k
                    definition_cursor= cur.execute(f"select get_ddl  ('table','{db_schema_table}');") 
                    results=cur.fetchall()
                    def_file_path=parent_dir+"\\"+ db_directory + "\\" +sc_directory + "\\" + 'Tables' + "\\" +k + "\\" + k +".txt"
                    print(def_file_path)
                    output="".join(results[0])
                    print(output)
                    with open(def_file_path, "w+") as f:
                        f.write(output)
                        f.close()
                        print("write done")
                 
        else:
             g=0
        
        
