import os
import json

import requests
import pandas as pd

CENSUS_API_KEY = os.environ.get('CENSUS_API_KEY')


class Dataset:
    # State code for Missouri
    state_code = 29
    # County code for STL City
    county_code = 510
    # Census API Key
    key = CENSUS_API_KEY

    local = None
    national = None

    def __init__(self, year, table, variable_map):
        self.url = 'https://api.census.gov/data/%s/acs/acs5' % year
        self.table = table
        self.variable_map = variable_map

        self.get_local()
        self.get_national()
        self.get_variables()

    def get_local(self):
        # https://api.census.gov/data/2020/acs/acs5?get=group(B02001)&for=block%20group:*&in=tract:*&in=state:29&in=county:510
        params = {
            "get": "group(%s)" % self.table,
            "for": "block group:*",
            "in": [
                "tract:*",
                "state:%s" % self.state_code,
                "county:%s" % self.county_code,
            ],
            "key": self.key
        }
        res = requests.get(self.url, params=params)
        data = res.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        df = df.dropna(axis=1, how='all')
        self.local = df

    def get_national(self):
        params = {
            "get": "group(%s)" % self.table,
            "for": "us:*",
        }
        res = requests.get(self.url, params=params)
        data = res.json()
        df = pd.DataFrame(data[1:], columns=data[0])
        df = df.dropna(axis=1, how='all')
        self.national = df

    def get_variables(self):
        result = {}
        variables = self.variable_map.get('variables')
        columns = self.local.columns.to_list()
        for column in columns:
            definition = variables.get(column)
            if definition:
                result[column] = definition
        
        self.variables = pd.DataFrame(result)


class ACSClient:
    variables_map = None
    tables = []

    def __init__(self, year):
        self.year = year
        self.geographies_url = 'https://api.census.gov/data/%s/acs/acs5/geography.json' % year
        self.variables_url = 'https://api.census.gov/data/%s/acs/acs5/variables.json' % year

        # Get variables data
        var_res = requests.get(self.variables_url)
        self.variables_map = var_res.json()


    def get(self, table):
        dataset = Dataset(self.year, table, self.variables_map)
        setattr(self, table, dataset)
        self.tables.append(table)


if __name__ == '__main__':
    client = ACSClient(2016)
    client.get('B02001')

    print(client.B02001.local)
    print(client.B02001.national)
    print(client.B02001.variables)
    