import os
import glob
import json

import pandas as pd


class Storage:

    def __init__(self):
        self.cache = os.path.join(os.path.expanduser('~'), ".gdelt-cache") 
        self.data = os.path.join(os.path.expanduser('~'), ".gdelt-data") 
        cache_exists = os.path.exists(self.cache)
        data_exists = os.path.exists(self.data)
        if not cache_exists:
            os.makedirs(self.cache)

        if not data_exists:
            os.makedirs(self.data)

    def create(self, hash, metadata={}):
        self.hash = hash
        self.cache_dir = "%s/%s" % (self.cache, self.hash)
        self.data_dir = "%s/%s" % (self.data, self.hash)
        cache_dir_exists = os.path.exists(self.cache_dir)
        data_dir_exists = os.path.exists(self.data_dir)

        if not cache_dir_exists:
            os.makedirs(self.cache_dir)

        if not data_dir_exists:
            os.makedirs(self.data_dir)

        with open("%s/metadata.json" % self.data_dir, 'w') as f:
            json.dump(metadata, f) 

    def put(self, data):
        item_fname = os.path.join(self.dir, "%s.json" % data.get('hash'))
        with open(item_fname, 'w') as f:
            json.dump(data, f)

    def put(self, data):
        item_fname = "%s/%s.json" % (self.cache_dir, data.get('hash'))
        with open(item_fname, 'w') as f:
            json.dump(data, f)


    def get(self, hash):
        item_fname = "%s/%s.json" % (self.cache_dir, hash)
        exists = os.path.exists(item_fname)
        if exists:
            with open(item_fname, 'r') as f:
                try:
                    data = json.load(f)
                    return data
                except Exception as e:
                    return None
        return None

    def list(self):
        metadata = glob.glob('%s/**/metadata.json' % (self.data))
        for item in metadata:
            with open(item, 'r') as f:
                yield json.load(f)

    def save(self, name, df):
        with open('%s/%s.parquet' % (self.data_dir, name), 'wb') as f:
            df.to_parquet(f)

    def load(self, name):
        with open('%s/%s.parquet' % (self.data_dir, name), 'wb') as f:
            return pd.read_parquet(f)

