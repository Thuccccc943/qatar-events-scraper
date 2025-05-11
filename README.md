# Qatar Events Scraper
A scraper made to fetch all events happening in Qatar from various sources. This data is taken for business analytics, intelligence and for fun purposes to aggregate everything that is going on.

# Currently supported sources:
- iloveqatar.com
- visitqatar.com
- qm.org.qa

# Features
- Flexible scraping configuration
- User friendly jupyter notebook for product managers
- Integration to Google Sheets
- [Planned] Duplicate handling
- [Planned] Duplicate handling cross websites

# Setup
## Install dependencies
```
$ pip install -r requirements.txt
```
## Setup Google Sheet Access
### Enable API Access
1. Head to [Google Developers Console](https://console.developers.google.com/) and create a new project, you can name it anything.
2. In the box labeled “Search for APIs and Services”, search for “Google Drive API” and enable it.
3. In the box labeled “Search for APIs and Services”, search for “Google Sheets API” and enable it.

### Create a service account
1. Go to “APIs & Services > Credentials” and choose “Create credentials > Service account key”.
2. Fill out the form.
3. Click “Create” and “Done”.
4. Press “Manage service accounts” above Service Accounts.
5. Press on ⋮ near recently created service account and select “Manage keys” and then click on “ADD KEY > Create new key”.
6. Select JSON key type and press “Create”.
7. Now you will be able to download this key which you just created.
8. Save this key in the same directory as this project and name it `credentials.json`
9. Very important! Go to your spreadsheet and share it with a `client_email` which is inside the `credentials.json` file. Just like you do with any other Google account.

# Running
## Interactive running
You can run the scraper via jupyter as follows
```
$ jupyter notebook
```

## Google Sheet Automation
There is already a workflow in the git repo and the script would run every hour, it is located at: `.github/workflows/run_script.yml`,
to setup automation on your own account clone this repo, see the **Setup Google Sheet Access** section first then come back to this part.
1. Go to your GitHub repo → Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name it CREDENTIALS_JSON and paste the entire content of your credentials.json file
4. Click "Add Secret"
