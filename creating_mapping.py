import json
import pandas as pd
from difflib import get_close_matches

path_raw = "datasets/part-00000-f5dff8e7-a8b7-4d08-b40b-4180858e3636-c000.csv"
path_clean = "stations.json"

# Load correct names
lines = {}
with open(path_clean) as jfile:
    lines = json.load(jfile)

stations = set([station for sublist in lines.values() for station in sublist])

# Load raw names
df = pd.read_csv(path_raw)
df.info()

raw_stations = df[df["nombreentidad"] == "METRO - OT"]["nombresitio"].unique()

# Create a list of transformation rules
rules = []
for raw_station in raw_stations:
    suggestions = get_close_matches(raw_station, stations)
    response = input("raw_station='%s' -- suggested=%s" % (raw_station, suggestions))
    if response == "":
        rules.append({
            "old-name": raw_station,
            "new-name": suggestions[0],
            "type": "automatic"
        })
    elif response.isdigit():
        rules.append({
            "old-name": raw_station,
            "new-name": suggestions[int(response)],
            "type": "pseudo-automatic"
        })
    else:
        rules.append({
            "old-name": raw_station,
            "new-name": response,
            "type": "manual"
        })

pd.DataFrame(rules).to_csv("station_rules.csv", index=False)
