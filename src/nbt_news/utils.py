from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

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
        maxrecords = 3000
    
    if start and end:
        timespan = None

    formatted_stations = " OR ".join(["station:%s" % station for station in stations])
    query = "%s (%s)" % (topic, formatted_stations) \
        if len(stations) > 1 \
        else "%s %s" % (topic, formatted_stations)

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

def get_weekly_ranges(start, end):
    start_date = datetime.strptime(start, "%Y%m%d%H%M%S")
    end_date = datetime.strptime(end, "%Y%m%d%H%M%S")
    ranges = []

    s = start_date
    e = datetime.strptime("01/01/1970", "%m/%d/%Y")
    while e < end_date:
        print(s, e, end_date)
        e = s + timedelta(days=6)
        ranges.append(
            (
                "%s000000" % s.strftime("%Y%m%d"), 
                "%s000000" % e.strftime("%Y%m%d")
            )
        )
        s = e 

    return ranges


# -------------------------------------------------------