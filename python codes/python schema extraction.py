import pandas as pd
from snowflake.connector import connect
import os
from github import Github
from github import Auth
from Github import github
from github import GithubIntegration
import base64
from github import Github
from pprint import pprint
import requests


# Authentication is defined via github.Auth
from github import Auth
import pandas.io.sql as psql

username = "Testaccountforcon"
password = "Merilytics&Employ123!@#"

#g = Github(username,password)
#login = requests.get('https://github.com/Testaccountforcon/Snowflake_con', auth=(username,password))
auth = Auth.Token("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")

# First create a Github instance:

# Public Web Github
g = Github(auth=auth)
for repo in g.get_user().get_repos():
    print(repo.name)
# repo = g.get_repo("Testaccountforcon/Snowflake_con")
# #repo.create_file("test.txt", "test", "test", branch="test")
# contents = repo.get_contents("")
# for content_file in contents:
#     print(content_file)
    
# repo.create_file("schema/test2.txt", "test", "test", branch="master")

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
parent_dir = 'Snowflake_con/schema'
for i in databases:
    db_directory = i
    
    #path = os.path.join(parent_dir, db_directory) 
    #os.mkdir(path)
    
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
    
    
    for j in df['name']:
        sc_directory = j
        #path = os.path.join(parent_dir, db_directory,sc_directory) 
        #os.mkdir(path)
        tablespath = (parent_dir + db_directory+sc_directory+'Tables')
        viewspath = (parent_dir+ db_directory+sc_directory+'Views')
        procedurespath = (parent_dir+ db_directory+sc_directory+'Procedures')
        
        
        #os.mkdir(viewspath)
        #os.mkdir(procedurespath)
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
                    #os.mkdir(path)
                    db_schema_table=i+"."+j+"."+k
                    definition_cursor= cur.execute(f"select get_ddl  ('table','{db_schema_table}');") 
                    results=cur.fetchall()
                    def_file_path=parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt"
                    print(def_file_path)
                    output="".join(results[0])
                    print(output)
                    #with open(def_file_path, "w+") as f:
                     #   f.write(output)
                      #  f.close()
                       # print("write done")
                    repo.create_file(def_file_path, "message", output, branch="master")
                    
                 
        else:
             g=0
        
        
