import pandas as pd
import os
from snowflake.connector import connect
import pandas as pd
from snowflake.connector import connect

from github import Github
from dotenv import load_dotenv

load_dotenv()


# Authentication is defined via github.Auth
from github import Auth

b = os.environ['GT_auth_token']
print(b)

auth = Auth.Token(b)

# First create a Github instance:

# Public Web Github
g = Github(auth=auth)
for repo in g.get_user().get_repos():
    print(repo.name)

connection = connect(
        user='Testaccountforcon',
        password='Merilytics&Employ123!@#',
        account='ocdaqur-ut77239',
        warehouse = 'COMPUTE_WH',
        database = 'DEMO_DB',
        #schema = 'PUBLIC'
    )
cur = connection.cursor()
databases = ['DEMO_DB']
parent_dir = 'Reference Schema'
for i in databases:
    db_directory = i
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
  
    
    for j in df['name']:
        sc_directory = j
        tablespath = (parent_dir + db_directory+sc_directory+'Tables')
        viewspath = (parent_dir+ db_directory+sc_directory+'Views')
        procedurespath = (parent_dir+ db_directory+sc_directory+'Procedures')
        db_schema=i +"." + j
        print (db_schema)
        tables_sql = cur.execute(f"show tables in {db_schema}")
        print(tables_sql)
        if cur.rowcount!=0:
            g=1
        else:
            g=0
        if cur.rowcount!=0:
             tables_df = pd.DataFrame(cur.fetchall())
             tables_df.columns = [column[0] for column in cur.description]
             for k in tables_df['name']:
                    path = (parent_dir+ db_directory+sc_directory+'Tables'+k)
                    db_schema_table=i+"."+j+"."+k
                    definition_cursor= cur.execute(f"select get_ddl  ('table','{db_schema_table}');") 
                    results=cur.fetchall()
                    def_file_path=parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt"
                    print(def_file_path)
                    output="".join(results[0])
                    print(output)
                    repo.create_file(def_file_path, "messagess", output, branch="master")
                  

for i in databases:
    db_directory = i
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
    
    for j in df['name']:
       if(j!='INFORMATION_SCHEMA'):
        sc_directory = j
        #tablespath = (parent_dir + db_directory+sc_directory+'Tables')
        viewspath = (parent_dir+ db_directory+sc_directory+'Views')
        #procedurespath = (parent_dir+ db_directory+sc_directory+'Procedures')
        db_schema=i +"." + j
        print (db_schema)
        views_sql = cur.execute(f"show views in {db_schema}")
        
        print(views_sql)

        if cur.rowcount!=0:
             views_df = pd.DataFrame(cur.fetchall())
             views_df.columns = [column[0] for column in cur.description]
             for k in views_df['name']:
                    path = (parent_dir+ db_directory+sc_directory+'Views'+k)
                    db_schema_view=i+"."+j+"."+k
                    definition_cursor= cur.execute(f"select get_ddl  ('view','{db_schema_view}');") 
                    results=cur.fetchall()
                    def_file_path=parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Views' + "/" +k + "/" + k +".txt"
                    print(def_file_path)
                    output="".join(results[0])
                    print(output)
                    repo.create_file(def_file_path, "message", output, branch="master")
                   
                     

for i in databases:
    db_directory = i
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
    
    for j in df['name']:
       if(j!='INFORMATION_SCHEMA'):
        sc_directory = j
        #tablespath = (parent_dir + db_directory+sc_directory+'Tables')
        #viewspath = (parent_dir+ db_directory+sc_directory+'Views')
        procedurespath = (parent_dir+ db_directory+sc_directory+'Procedures')
        db_schema=i +"." + j
        print (db_schema)
        procedures_sql = cur.execute(f"show views in {db_schema}")
        
        print(procedures_sql)

        if cur.rowcount!=0:
             procedures_df = pd.DataFrame(cur.fetchall())
             procedures_df.columns = [column[0] for column in cur.description]
             for k in procedures_df['name']:
                    path = (parent_dir+ db_directory+sc_directory+'Procedures'+k)
                    db_schema_procedures=i+"."+j+"."+k
                    definition_cursor= cur.execute(f"select get_ddl  ('Procedures','{db_schema_procedures}');") 
                    results=cur.fetchall()
                    def_file_path=parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Procedures' + "/" +k + "/" + k +".txt"
                    print(def_file_path)
                    output="".join(results[0])
                    print(output)
                    repo.create_file(def_file_path, "message", output, branch="master")
                   
               
    
        
        
