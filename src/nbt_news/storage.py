import os
import json
import logging

import s3fs
import pandas as pd


class Storage:
    hash = None
    dir = None

    def __init__(self, hash, metadata={}):
        self.dir = "./.cache/%s" % hash
        exists = os.path.exists(self.dir)
        if not exists:
            os.makedirs(self.dir)

        with open(os.path.join(self.dir, 'metadata.json'), 'w') as f:
            json.dump(metadata, f)

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


class Minio:
    hash = None
    dir = None

    def __init__(self, bucket=None):
        self.fs = s3fs.S3FileSystem(
            anon=False,
            key=os.environ.get("MINIO_ACCESS_KEY"),
            secret=os.environ.get("MINIO_SECRET_KEY"),
            client_kwargs={ "endpoint_url": "http://api.minio.cluster1.nobedtimes.com" },
        )
        self.bucket = "news-analysis-cache" if not bucket else bucket

    def create(self, hash, metadata={}):
        self.dir = "%s/%s" % (self.bucket, hash)
        exists = self.fs.exists(self.dir)
        if not exists:
            self.fs.makedirs(self.dir)

        with self.fs.open("%s/metadata.json" % self.dir, 'w') as f:
            json.dump(metadata, f) 


    def put(self, data):
        item_fname = "%s/%s.json" % (self.dir, data.get('hash'))
        with self.fs.open(item_fname, 'w') as f:
            json.dump(data, f)

    def get(self, hash):
        item_fname = "%s/%s.json" % (self.dir, hash)
        exists = self.fs.exists(item_fname)
        if exists:
            with self.fs.open(item_fname, 'r') as f:
                try:
                    data = json.load(f)
                    return data
                except Exception as e:
                    return None
        return None

    def list(self):
        metadata = self.fs.glob('%s/**/metadata.json' % self.bucket)
        for item in metadata:
            with self.fs.open(item, 'r') as f:
                yield json.load(f)

    def save(self, name, df):
        with self.fs.open('%s/%s/%s.parquet' % (self.bucket, self.hash, name), 'wb') as f:
            df.to_parquet(f)

    def load(self, name):
        with self.fs.open('%s/%s/%s.parquet' % (self.bucket, self.hash, name), 'wb') as f:
            return pd.read_parquet(f)