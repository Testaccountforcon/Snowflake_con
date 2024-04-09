import pandas as pd
import os
from snowflake.connector import connect
import pandas as pd
from snowflake.connector import connect

from github import Github
from dotenv import load_dotenv
import filecmp
load_dotenv()
from github import GithubIntegration

# Authentication is defined via github.Auth
from github import Auth



# Authentication is defined via github.Auth
#print(os.environ.get("GT_AUTH_TOKEN"))
 # username =os.environ.get("GT_USERNAME", "<unknown>")
 # password = os.environ.get("GT_PASSWORD", "<unknown>")
 # auth =os.environ.get("GT_AUTH_TOKEN", "<unknown")

 #username = "Testaccountforcon"
 #password = "Merilytics&Employ123!@#"
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
parent_dir = 'latest schema pull'


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
            f=1
        else:
            f=0
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
                    # file_1 = "Snowflake_con/" + def_file_path
                    # file_2  = "Reference Schema" + "/"+ db_directory + "/" +sc_directory + "/" + 'Tables' + "/" +k + "/" + k +".txt"
                    # result_deep = filecmp.cmp(file_1, file_2, shallow=False)
                    # if( result_deep == False):
                    #  a=a.append(file_1)
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
jobstatus="Success"
if(a==[]):
 jobstatus="Success"
else:
 jobstatus = "Fail"


env_file = os.getenv('GITHUB_ENV')

bar='bar'

with open(env_file, "a") as myfile:
    myfile.write(f"foo={jobstatus}")
 #print(f"::set-output name=test_report::{jobstatus}")
# print(f"::set-output name=j_status::{jobstatus}")
# print(jobstatus)


