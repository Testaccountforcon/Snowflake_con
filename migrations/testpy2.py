import pandas as pd
import os
from snowflake.connector import connect
from github import Github
from dotenv import load_dotenv
load_dotenv()
from github import GithubIntegration

# Authentication is defined via github.Auth
from github import Auth
b = os.environ['GT_auth_token']
print(b)
 #g = Github(username,password)
 #login = requests.get('https://github.com/Testaccountforcon/Snowflake_con', auth=(username,password))
auth = Auth.Token(b)
a=[]
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
parent_dir = 'latest schema pull'


for i in databases:
    db_directory = i
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
    
    for j in df['name']:
        sc_directory = j
        tablespath = (parent_dir + db_directory+sc_directory+'Tables')
        #viewspath = (parent_dir+ db_directory+sc_directory+'Views')
        #procedurespath = (parent_dir+ db_directory+sc_directory+'Procedures')
        db_schema=i +"." + j
        print (db_schema)
        tables_sql = cur.execute(f"show tables in {db_schema}")
        print(tables_sql)

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
                    repo.create_file(def_file_path, "message", output, branch="master")
                    repo_name = g.get_user().get_repo("Snowflake_con")
                    contents_1 = repo_name.get_contents(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt")
                    contents_2 = repo_name.get_contents("Reference Schema"+"/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt")
                    if(contents_1.decoded_content.decode() == contents_2.decoded_content.decode()):
                     print("match")
                    else:
                     a.append(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt")
                     print ("Error")
                     
                    
                 
        else:
             f=0
jobstatus=""
if(a==[]):
 jobstatus=""
else:
 jobstatus = "change"
s=''.join(a)
repo.create_file("migrations/chemacheck.txt", "jobstatus", jobstatus, branch="master")
repo.create_file("migrations/diff.txt", "diff", s, branch="master")



