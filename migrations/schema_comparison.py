import pandas as pd
import os
from snowflake.connector import connect
from github import Github
from dotenv import load_dotenv
from ffcount import ffcount
load_dotenv()
from github import GithubIntegration

# Authentication is defined via github.Auth
from github import Auth
b = os.environ['GT_auth_token']
print(b)
auth = Auth.Token(b)
a=[]
g = Github(auth=auth)
for repo in g.get_user().get_repos():
    print(repo.name)
 

connection = connect(
        user=os.environ["SF_USERNAME"],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SF_ACCOUNT'],
        warehouse = os.environ['SF_WAREHOUSE'],
        database = os.environ['SF_DATABASE'],
    )
cur = connection.cursor()
databases = ['DEMO_DB']
parent_dir = 'latest-schema-pull'

def count_files_in_directory(directory_path):
    num_files, _ = ffcount(directory_path)
    return num_files



for i in databases:
    db_directory = i
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
    
    for j in df['name']:
        sc_directory = j
        tablespath = (parent_dir + db_directory+sc_directory+'Tables')
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
                    try:
                     contents_2 = repo_name.get_contents("reference-schema"+"/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt")
                    except:
                     a.append(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt")
                     continue
                    if(contents_1.decoded_content.decode() == contents_2.decoded_content.decode()):
                     print("match")
                    else:
                     a.append(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt")
                     print ("Error")
                     
                    
                 
        else:
             f=0

for i in databases:
    db_directory = i
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
    
    for j in df['name']:
       if(j!='INFORMATION_SCHEMA'):
        sc_directory = j
       
        viewspath = (parent_dir+ db_directory+sc_directory+'Views')
       
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
                    repo_name = g.get_user().get_repo("Snowflake_con")
                    contents_1 = repo_name.get_contents(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Views' + "/" +k + "/" + k +".txt")
                    try:
                     contents_2 = repo_name.get_contents("reference-schema"+"/"+ db_directory + "/" +sc_directory + "/" + 'Views' + "/" +k + "/" + k +".txt")
                    except:
                     a.append(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Views' + "/" +k + "/" + k +".txt")
                     continue
                    if(contents_1.decoded_content.decode() == contents_2.decoded_content.decode()):
                     print("match")
                    else:
                     a.append(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Views' + "/" +k + "/" + k +".txt")
                     print ("Error")
                     

for i in databases:
    db_directory = i
    sql = cur.execute(f"show schemas in {i}")
    df = pd.DataFrame(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    print(df)
    
    for j in df['name']:
       if(j!='INFORMATION_SCHEMA'):
        sc_directory = j
      
        procedurespath = (parent_dir+ db_directory+sc_directory+'Procedures')
        db_schema=i +"." + j
        print (db_schema)
        procedures_sql = cur.execute(f"show procedures in {db_schema}")
        
        print(procedures_sql)

        if cur.rowcount!=0:
            
             procedures_df = pd.DataFrame(cur.fetchall())
             procedures_df.columns = [column[0] for column in cur.description]
             for k in procedures_df['name']:
                   if(k!='SYSTEM$SEND_EMAIL'):
                    path = (parent_dir+ db_directory+sc_directory+'Procedures'+k)
                    db_schema_procedures=i+"."+j+"."+k
                    definition_cursor= cur.execute(f"select get_ddl  ('procedure','{db_schema_procedures}');") 
                    results=cur.fetchall()
                    def_file_path=parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Procedures' + "/" +k + "/" + k +".txt"
                    print(def_file_path)
                    output="".join(results[0])
                    print(output)
                    repo.create_file(def_file_path, "message", output, branch="master")
                    repo_name = g.get_user().get_repo("Snowflake_con")
                    contents_1 = repo_name.get_contents(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Procedures' + "/" +k + "/" + k +".txt")
                    try:
                     contents_2 = repo_name.get_contents("reference-schema"+"/"+ db_directory + "/" +sc_directory + "/" + 'Procedures' + "/" +k + "/" + k +".txt")
                    except:
                     a.append(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Procedures' + "/" +k + "/" + k +".txt")
                     continue
                    if(contents_1.decoded_content.decode() == contents_2.decoded_content.decode()):
                     print("match")
                    else:
                     a.append(parent_dir+"/"+ db_directory + "/" +sc_directory + "/" + 'Procedures' + "/" +k + "/" + k +".txt")
                     print ("Error")
               
           

 

jobstatus=""
if(a==[]):
 jobstatus=""
else:
 jobstatus = "Definition Mismatch"
 
s=''.join(a)
repo.create_file("migrations/schemacheck.txt", "jobstatus", jobstatus, branch="master")




