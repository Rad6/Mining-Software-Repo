from utils import *
import json


if __name__ == "__main__":

    with open('config.json') as config_file:
        config = json.load(config_file)

    SCM_url = config['SCM_url']
    ITS_url = config['ITS_url']
    local_repo_dir = config['local_repo_dir']
    project = config['project']
    issuetype = config['issuetype']
    status = config['status']
    resolution = config['resolution']
    until_day = config['until_day']

    clone_or_pull_remote_repo(SCM_url)
    get_issues(ITS_url, project, issuetype, status, resolution, until_day)
    get_commits(local_repo_dir)
    create_dataset()