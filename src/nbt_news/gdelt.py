import os
import sys
import json
import time
import hashlib
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd

from .log import logging
from .utils import *
from .storage import Minio
from .nlp import pipeline


GDELT_URL = "https://api.gdeltproject.org/api/v2/tv/tv"



# -------------------------------------------------------
# GDELT and News Analysis SDK
# -------------------------------------------------------


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
            time.sleep(.5)
            logging.info("Requesting data for item %s from API" % self.hash)
            logging.debug(query)

            res = requests.get(GDELT_URL, params=query) 
            self.query = query
            self.status_code = res.status_code
            self.raw_response = res.text
            logging.debug(self.status_code, self.query)
            try:
                raw_data = res.json()
                self.data = hook(raw_data)
            except Exception as e:
                self.error = e
                logging.error("Loading data for %s failed: %s" % (self.hash, e))
                logging.debug(res.status_code)
                logging.debug(res.text)
            
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
    
    def __init__(self, store, topic, stations, start=None, end=None, resolution='monthly'):
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

    def __init__(self, store, topic, stations, start=None, end=None, resolution='monthly'):
        self.store = store

        self.topic = topic
        self.stations = stations
        self.start = start
        self.end = end
        self.resolution = resolution

        self.get()

    def get(self):
        for station in self.stations:
            ranges = get_monthly_ranges(self.start, self.end) if self.resolution == 'monthly' else get_weekly_ranges(self.start, self.end)
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

    def __init__(self, store=None):
        if store:
            self.store = store
        else:
            self.store = Minio()

    def _get_volume(self):
        if not self.hash:
            raise Exception("You must create a search before attempting to fetch data")

        logging.info("Getting volume dataset...")
        volume = VolumeDataset(
            self.store,
            self.topic,
            self.stations,
            self.start,
            self.end
        )
        self.volume = volume

    def _get_clips(self):
        if not self.hash:
            raise Exception("You must create a search before attempting to fetch data")

        logging.info("Getting clips dataset...")
        clips = ClipsDataset(
            self.store,
            self.topic,
            self.stations,
            self.start,
            self.end
        )
        self.clips = clips

    def search(
            self, 
            topic, 
            stations, 
            start=None, 
            end=None,
        ):
        # Base Settings
        self.topic = topic
        self.stations = stations
        self.start = convert_date(start)
        self.end = convert_date(end)

        self.metadata = {
            "topic": topic,
            "stations": stations,
            "start": start,
            "end": end,
        }
        
        # Hash
        self.hash = hashlib.sha224(
            str.encode(json.dumps({
                "topic": self.topic, 
                "stations": '-'.join(self.stations), 
                "start": self.start,
                "end": self.end
            }, sort_keys=True))
        ).hexdigest()

        # Log our hash value
        logging.info("Initialized project: %s Topic: %s Stations: %s Dates: %s - %s" % (
            self.hash, 
            self.topic, 
            ', '.join(self.stations),
            self.start,
            self.end
        ))

        # Storage and init
        self.store.create(self.hash, metadata=self.metadata)
        self._get_volume()
        self._get_clips()

    def preprocess(self):
        for item in self.stations:
            df = getattr(self.clips, item.lower())
            df = pipeline(df)
            self.store.save(item.lower(), df)

    def list(self):
        metadata = list(self.store.list())
        return metadata



if __name__ == "__main__":
    news = NewsAnalysis()
    item = news.list()[0]
    news.search(**item)
    news.preprocess()
