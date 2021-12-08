from jira import JIRA
import dateutil.parser
from datetime import datetime, timezone
from pydriller import Repository
import pandas as pd
from git import Repo
import os


def clone_or_pull_remote_repo(git_url):
    """if the repo is available on local machine, it 
    pulls from it, otherwise, it clones the whole repo"""

    absolute_path = os.path.dirname(__file__)
    local_repo_path = os.path.join(absolute_path, 'local_repo')

    if not os.path.isdir('local_repo'):
        print("cloning remote repo...")
        Repo.clone_from(git_url, local_repo_path)
    else:
        print("pulling from remote repo...")
        repo = Repo(local_repo_path)
        origin = repo.remotes.origin
        origin.pull()


def generate_query(project, issuetype, status, resolution, until_day):
    """generates a JIRA search query with the given parameters"""

    query = ""

    query += "project in ("
    query += ','.join(project)
    query += ") AND "

    query += "issuetype in ("
    query += ','.join(issuetype)
    query += ") AND "
    
    query += "status in ("
    query += ','.join(status)
    query += ") AND "

    query += "resolution in ("
    query += ','.join(resolution)
    query += ") AND "

    query += "created < startOfDay(-" + str(until_day) + "d)"

    query += " ORDER BY created DESC"

    return query



def get_issues(url, project, issuetype, status, resolution, until_day):
    """retrieves and stores the JIRA issues. Since JIRA servers limit requesting 
    issues to 1000 at a time, we have to send iterative requests."""

    print("getting issues from JIRA...")
    jira_amq = JIRA(url)
    issues = []

    while True:
        search_query = generate_query(project, issuetype, status, resolution, until_day)
        print("processing query:")
        print(search_query)
        next_issues_packet = jira_amq.search_issues(search_query, maxResults=1000)
        issues.extend(next_issues_packet)
        
        if len(next_issues_packet) < 1000:
            break

        last_retrieved_issue = next_issues_packet[-1]
        created_on = dateutil.parser.isoparse(last_retrieved_issue.fields.created)
        now = datetime.now(timezone.utc)
        days_diff = (now - created_on).days
        until_day = days_diff
    
    df = pd.DataFrame(columns=['ITS_id','ITS_key','ITS_summary','ITS_description'])
    for issue in issues:
        if len(df) != 0 and df.loc[len(df) - 1]['ITS_id'] == issue.id:
            continue
        df.loc[len(df)] = [issue.id, issue.key, issue.fields.summary, issue.fields.description]
    
    if not os.path.isdir('output'):
        absolute_path = os.path.dirname(__file__)
        output_path = os.path.join(absolute_path, 'output')
        os.mkdir(output_path)
    df.to_csv('output/issues.csv', encoding="utf-8", index=False)


def get_commits(local_repo_dir):
    """retrieves and stores commits that are only related to a 
    JIRA issue of the project and only contain added lines"""

    print("getting commits from git...")
    df = pd.DataFrame(columns=['SCM_hash','SCM_message'])
    for commit in Repository(local_repo_dir , since=datetime(2000, 1, 1, 1, 0, 0)).traverse_commits():

        if "AMQ" not in commit.msg:
            continue
        added_lines = 0
        deleted_lines = 0
        for file in commit.modified_files:
            added_lines += file.added_lines
            deleted_lines += file.deleted_lines
        
        if deleted_lines == 0 and added_lines > 0:
            df.loc[len(df)] = [commit.hash, commit.msg]

    df.to_csv('output/commits.csv', encoding="utf-8", index=False)


def create_dataset():
    """performs a join operation in order to find issues' respective 
    commits, which results in final dataset"""

    print("joining issues with their corresponding commit...")
    issues = pd.read_csv("output/issues.csv")
    commits = pd.read_csv("output/commits.csv")

    final_dataset = pd.DataFrame(columns=['ITS_id','ITS_key','ITS_summary','ITS_description', 'SCM_hash', 'SCM_msg'])
    for i in range(len(issues)):
        for j in range(len(commits)):
            ITS_key = issues.iloc[i, issues.columns.get_loc('ITS_key')]
            SCM_msg = commits.iloc[j, commits.columns.get_loc('SCM_message')]
            index = SCM_msg.find(ITS_key)
            if index == -1:
                continue
            if len(SCM_msg) > index + len(ITS_key) and SCM_msg[index + len(ITS_key)].isnumeric():
                continue
            final_dataset.loc[len(final_dataset)] = [
                issues.iloc[i, issues.columns.get_loc('ITS_id')],
                issues.iloc[i, issues.columns.get_loc('ITS_key')],
                issues.iloc[i, issues.columns.get_loc('ITS_summary')],
                issues.iloc[i, issues.columns.get_loc('ITS_description')],
                commits.iloc[j, commits.columns.get_loc('SCM_hash')],
                commits.iloc[j, commits.columns.get_loc('SCM_message')]
            ]

    final_dataset.to_csv("output/final_dataset.csv", encoding="utf-8", index=False)
    print("saved in output directory.")