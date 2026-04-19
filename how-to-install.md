# How to Install the Necessary Software Tools

## In order to set up your environment to run the etl scripts or to run backups, follow these steps:

1. Install python3.9 (see https://www.python.org/downloads/)
2. Install and activate pyvenv (optional - see https://docs.python.org/3/library/venv.html)
3. Install pip (see https://pip.pypa.io/en/stable/installing/)
4. Clone this repo and navigate to it in a terminal window (e.g., bash)
5. Copy your secrets file to <ROOT>etl/secrets.json<ROOT>, in whatever folder you cloned the repo in. The secrets.json file will contain the API keys for the institutions you are accessing. You'll need to work with these institutions (or another rhizomes developer) to obtain these keys. An example secrets file is below.

Example Secrets file:
  ```
  {
    "apis": {
        "keys": {
            "dpla": "YOUR-KEY-HERE",
            "smithsonian": "YOUR-KEY-HERE",
            "calisphere": "YOUR-KEY-HERE"
        }
    }
}
```

6. Install required modules:

```
pip install -r requirements.txt
```
