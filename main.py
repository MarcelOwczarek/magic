import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time, random

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8"
})

airports = ["WAW","WMI","KRK","LCJ","KTW","POZ"]

start = datetime.today()
end = datetime(start.year, 8, 31)

def build_link(o, d, date):
    return f"https://www.ryanair.com/pl/pl/trip/flights/select?adults=1&dateOut={date}&originIata={o}&destinationIata={d}&isReturn=false"

def get_destinations(origin):
    url = f"https://www.ryanair.com/api/views/locate/searchWidget/routes/en/airport/{origin}"
    try:
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            return []
        return list(set(x["arrivalAirport"]["code"] for x in r.json() if "arrivalAirport" in x))
    except:
        return []

routes = {a: get_destinations(a) for a in airports}

days = [(start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end - start).days + 1)]

def fetch(o, d, date):
    url = "https://www.ryanair.com/api/farfnd/v4/oneWayFares"
    params = {
        "departureAirportIataCode": o,
        "arrivalAirportIataCode": d,
        "outboundDepartureDateFrom": date,
        "outboundDepartureDateTo": date,
        "market": "pl-pl",
        "adultPaxCount": 1
    }

    for _ in range(3):
        try:
            r = session.get(url, params=params, timeout=10)
            if r.status_code != 200:
                return []
            data = r.json()
            return [{
                "data": f["outbound"]["departureDate"][:10],
                "godzina": f["outbound"]["departureDate"][11:16],
                "wylot": o,
                "przylot": d,
                "cena": f["outbound"]["price"]["value"],
                "waluta": f["outbound"]["price"]["currencyCode"],
                "link": build_link(o, d, f["outbound"]["departureDate"][:10])
            } for f in data.get("fares", [])]
        except:
            time.sleep(1)
    return []

def process_route(pair):
    o, d = pair
    res = []
    for day in days:
        res += fetch(o, d, day)
        res += fetch(d, o, day)
        time.sleep(0.1)
    return res

pairs = [(o, d) for o in airports for d in routes[o]]

all_data = []
with ThreadPoolExecutor(max_workers=3) as ex:
    for r in ex.map(process_route, pairs):
        all_data.extend(r)

df = pd.DataFrame(all_data)

if len(df):
    df = df.drop_duplicates().sort_values(["data", "cena"])
    df.to_csv("final_boss.csv", index=False)
    print("DONE:", len(df))
else:
    print("Brak danych")
