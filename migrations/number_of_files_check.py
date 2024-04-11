
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
# First create a Github instance:

# Public Web Github
g = Github(auth=auth)
for repo in g.get_user().get_repos():
    print(repo.name)

def count_files_in_directory(directory_path):
    num_files, _ = ffcount(directory_path)
    return num_files

if(count_files_in_directory("latest schema pull") != count_files_in_directory("Reference Schema"):
  repo = g.get_user().get_repo("Snowflake_con")
  file = repo.get_file_contents("migrations/schemacheck.txt")

# update
  repo.update_file("migrations/schemacheck.txt", "your_commit_message", "Untracked files in Snowflake", file.sha)

