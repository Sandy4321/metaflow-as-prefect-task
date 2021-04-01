"""

    Stand-alone script to test the interaction with Github API

    Prefect has *some* GitHub actions ready
    (e.g. https://github.com/PrefectHQ/prefect/blob/05cac2372c57a93ea72b05e7c844b1e115c01047/src/prefect/tasks/github/prs.py#L8)
    but not the full clone.

"""
import os
from dotenv import load_dotenv
# load envs
load_dotenv(verbose=True, dotenv_path='github.env')
import requests
import zipfile
from io import BytesIO
import tempfile

url = "https://api.github.com/repos/{}/{}/zipball".format(
    os.getenv('REPO_OWNER'), os.getenv('REPO_NAME')
)
headers = {
    "AUTHORIZATION": "token {}".format(os.getenv('GITHUB_TOKEN')),
    "Accept": "application/vnd.github.v3+json",
}

print(url)
# send the request
request = requests.get(url, headers=headers)
zip_name = request.headers.get("Content-Disposition").split("filename=")[1]
print("Downloading file: {}".format(zip_name))
# read the zip file in memory
file = zipfile.ZipFile(BytesIO(request.content))
file.extractall('test')
# extract zip content to temp file, to clean it up
with tempfile.TemporaryDirectory() as tmpdirname:
    print("Temp directory is: {}".format(tmpdirname))
    file.extractall(tmpdirname)