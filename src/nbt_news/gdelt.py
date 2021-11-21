import os
import sys
import json
import time
import logging
import hashlib
from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd

GDELT_URL = "https://api.gdeltproject.org/api/v2/tv/tv"

# Logging
# -----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M',
    # handlers=[logging.StreamHandler(sys.stdout)]
    filename='./news_analysis.log',
    filemode='a'
)
# -----------------------------------------------------


# GDELT Utility Functions
# -------------------------------------------------------

# Create Query for GDELT API
def create_query(
    topic,
    stations,
    mode,
    format="json",
    timespan="FULL",
    last24="yes",
    timezoom=None,
    sort=None,
    maxrecords=None,
    start=None,
    end=None
):
    if mode == "timelinevol":
        timezoom = "yes"
    
    if mode == "clipgallery":
        sort = "datedesc"
        maxrecords = 5000
    
    if start and end:
        timespan = None

    formatted_stations = " OR ".join(["station:%s" % station for station in stations])
    query = "%s (%s)" % (topic, formatted_stations)

    base_params = {
        "format": format,
        "timespan": timespan,
        "last24": last24,
        "timezoom": timezoom,
        "mode": mode,
        "sort": sort,
        "maxrecords": maxrecords,
        "query": query,
        "startdatetime": start,
        "enddatetime": end,
    }

    return { k: v for k, v in base_params.items() if v is not None }

def restructure_clips(data):
    return data.get('clips')

def restructure_timelines(data):
    records = data['timeline']
    processed = [
        [
            {
                "station": record['series'],
                "date": item['date'],
                "value": item['value']
            } for item in record['data']
        ] for record in records
    ]
    return [item for sublist in processed for item in sublist]

def convert_date(date_string):
    if date_string:
        date_obj = datetime.strptime(date_string, "%m/%d/%Y")
        return "%s000000" % date_obj.strftime("%Y%m%d")
    return None

def get_monthly_ranges(start, end):
    start_date = datetime.strptime(start, "%Y%m%d%H%M%S")
    end_date = datetime.strptime(end, "%Y%m%d%H%M%S")

    num_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
    ranges = []
    if num_months > 1:
        for i in range(num_months):
            d = start_date + relativedelta(months=i)
            last_day = d + relativedelta(day=1, months=+1, days=-1)
            first_day = d + relativedelta(day=1)
            ranges.append(
                (
                    "%s000000" % first_day.strftime("%Y%m%d"), 
                    "%s000000" % last_day.strftime("%Y%m%d")
                )
            )
    else: 
        last_day = start_date + relativedelta(day=1, months=+1, days=-1)
        first_day = start_date + relativedelta(day=1)
        ranges.append(
            (
                "%s000000" % first_day.strftime("%Y%m%d"), 
                "%s000000" % last_day.strftime("%Y%m%d")
            )
        )
    return ranges
# -------------------------------------------------------


# -------------------------------------------------------
# GDELT and News Analysis SDK
# -------------------------------------------------------
class Storage:
    hash = None
    dir = None

    def __init__(self, hash):
        self.dir = "./.cache/%s" % hash
        exists = os.path.exists(self.dir)
        if not exists:
            os.makedirs(self.dir)

    def put(self, data):
        item_fname = os.path.join(self.dir, "%s.json" % data.get('hash'))
        with open(item_fname, 'w') as f:
            json.dump(data, f)

    def get(self, hash):
        item_fname = os.path.join(self.dir, "%s.json" % hash)
        exists = os.path.exists(item_fname)
        if exists:
            with open(item_fname, 'r') as f:
                try:
                    data = json.load(f)
                    return data
                except Exception as e:
                    logging.error("Loading data from file %s failed: %s" % (item_fname, e))
                    return None
        return None


class CacheItem:
    storage = None

    hash = None
    query = None
    status_code = None
    raw_response = None
    data = None
    error = None

    def __init__(
        self, 
        storage, 
        query,
        hook=lambda x: x
    ):
        """Create cache item for storage"""
        self.storage = storage
        self.hash = hashlib.sha224(
            str.encode(json.dumps(query, sort_keys=True))
        ).hexdigest()

        cached = self.load()
        if not cached:
            time.sleep(5)
            logging.info("Requesting data for item %s from API" % self.hash)

            res = requests.get(GDELT_URL, params=query) 
            self.query = query
            self.status_code = res.status_code
            self.raw_response = res.text
            try:
                raw_data = res.json()
                self.data = hook(raw_data)
            except Exception as e:
                self.error = e
                logging.error("Loading data for %s failed: %s" % (self.hash, e))
            
            self.store()
        if cached:
            logging.info("Loaded data for item %s from cache" % self.hash)

    def store(self):
        """Send to Storage Class"""
        data = self.serialize()
        self.storage.put(data)

    def load(self):
        """Load from Storage Class"""
        data = self.storage.get(self.hash)
        if data:
            self.deserialize(data)
            return True
        return False

    def serialize(self):
        return {
            "hash": self.hash,
            "query": self.query,
            "status_code": self.status_code,
            "raw_response": self.raw_response,
            "data": self.data,
            "error": "%s" % self.error
        }

    def deserialize(self, data):
        self.hash = data.get('hash')
        self.query = data.get('query')
        self.status_code = data.get('status_code')
        self.raw_response = data.get('raw_response')
        self.data = data.get('data')
        self.error = data.get('error')

    def df(self):
        if self.data:
            df = pd.DataFrame(self.data) 
            df['date'] = pd.to_datetime(df['date'])
            return df
        return None


class VolumeDataset:
    store = None

    topic = None
    stations = None
    start = None
    end = None
    datasets = None

    all = None
    
    def __init__(self, store, topic, stations, start=None, end=None):
        self.store = store

        self.topic = topic
        self.stations = stations
        self.start = start
        self.end = end

        self.get()

    def get(self):
        # Set our query
        query = create_query(
            self.topic, 
            self.stations, 
            'timelinevol',
            start=self.start,
            end=self.end
        )

        # Create a cached Item for combined
        # Station data
        cache = CacheItem(
            self.store, 
            query,
            restructure_timelines
        ) 
        # Set our local dataframe
        self.all = cache.df()
        
        for station in self.stations:
            # Check for cached item here
            # If cached item exists, load it

            # else, make a new request
            station_query = create_query(
                self.topic, 
                [station], 
                'timelinevol',
                start=self.start,
                end=self.end
            )
            # This should be a raw response
            station_cache = CacheItem(
                self.store,
                station_query,
                restructure_timelines
            ) 

            # Finally, set the attribute with a dataframe
            setattr(
                self, 
                station.lower(), 
                station_cache.df()
            )

        # Record which datasets we have stored in memory
        self.datasets = [i.lower() for i in self.stations]

        # Log a successful run
        logging.info("Created Volume datasets for %s" % ', '.join(self.stations))


class ClipsDataset:
    store = None

    topic = None
    stations = None
    start = None
    end = None
    datasets = None

    def __init__(self, store, topic, stations, start=None, end=None):
        self.store = store

        self.topic = topic
        self.stations = stations
        self.start = start
        self.end = end

        self.get()

    def get(self):
        for station in self.stations:
            ranges = get_monthly_ranges(self.start, self.end)
            dfs = []
            for range in ranges:
                # Set our query
                query = create_query(
                    self.topic, 
                    [station], 
                    'clipgallery',
                    start=range[0],
                    end=range[1]
                )
                station_cache = CacheItem(
                    self.store,
                    query,
                    restructure_clips 
                ) 

                # Finally, set the attribute with a dataframe
                dfs.append(station_cache.df())

            setattr(
                self, 
                station.lower(), 
                pd.concat([df for df in dfs if df is not None])
            )

        # Record which datasets we have stored in memory
        self.datasets = [i.lower() for i in self.stations]

        # Log a successful run
        logging.info("Created Clip datasets for %s" % ', '.join(self.stations))




class NewsAnalysis:
    store = None

    hash = None
    topic = None
    stations = None
    start = None
    end = None

    volume = None
    clips= None

    def __init__(
            self, 
            topic, 
            stations, 
            start=None, 
            end=None,
            store=None,
        ):
        # Base Settings
        self.topic = topic
        self.stations = stations
        self.start = convert_date(start)
        self.end = convert_date(end)
        
        # Hash
        self.hash = hashlib.sha224(
            str.encode(json.dumps({
                "topic": self.topic, 
                "stations": '-'.join(self.stations), 
                "start": self.start,
                "end": self.end
            }, sort_keys=True))
        ).hexdigest()

        # Storage and init
        self.store = Storage(self.hash)
        self.get()

    def get(self):
        volume = VolumeDataset(
            self.store,
            self.topic,
            self.stations,
            self.start,
            self.end
        )
        self.volume = volume
        clips = ClipsDataset(
            self.store,
            self.topic,
            self.stations,
            self.start,
            self.end
        )
        self.clips = clips