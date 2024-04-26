
import pandas as pd
import os
from github import Github
from ffcount import ffcount
from github import GithubIntegration
from github import Auth
# Authentication is defined via github.Auth
# Logging into GitHub repo using Authentication token

b = os.environ['GT_auth_token']
print(b)
auth = Auth.Token(b)
g = Github(auth=auth)
for repo in g.get_user().get_repos():
    print(repo.name)

# This function calculates the number of definitions in reference-schema against number of definitions in latest-schema-pull and stores the result in schemacheck.txt.
# If number of definitions mismatch, a custom message is written in schemacheck.txt else the file is empty. 
def count_files_in_directory(directory_path):
    num_files, _ = ffcount(directory_path)
    return num_files

if(count_files_in_directory("latest-schema-pull") != count_files_in_directory("reference-schema")):
  repo = g.get_user().get_repo("Snowflake_con")
  contents = repo.get_contents("migrations/schemacheck.txt")  
  repo.update_file("migrations/schemacheck.txt","updating","Untracked files in Snpwflake",contents.sha)



