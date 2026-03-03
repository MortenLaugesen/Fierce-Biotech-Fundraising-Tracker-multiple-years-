import yaml, requests, pandas as pd
from parsers import extract_entries
from datetime import datetime

HEADERS = {"User-Agent": "fundraise-tracker-bot/1.0 (+internal dashboards)"}

def fetch(url: str):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def main():
    with open("trackers.yaml","r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    rows = []
    for t in cfg["trackers"]:
        html = fetch(t["url"])
        rows += extract_entries(html, t["year"], t["url"])
    df = pd.DataFrame(rows)
    return df

if __name__ == "__main__":
    df = main()
    # Load to Snowflake
    from snowflake_load import upsert
    upsert(df, table="BIOTECH.FUNDRAISING.FUNDRAISERS")
