from logger import*
from decimal import Decimal
from collections import defaultdict

CACHE_IN_SECONDS = 60

last_timestamp = {}
last_timestamp = defaultdict(lambda:None, last_timestamp)

cache = {}

def valid_timestamp_for_add(old_ts, new_ts):
    return new_ts >= (old_ts + (CACHE_IN_SECONDS - (Decimal(old_ts) % CACHE_IN_SECONDS) ) )

def valid_timestamp_for_get(old_ts, new_ts):
    return (new_ts >= old_ts) and (new_ts <= (old_ts + CACHE_IN_SECONDS))

def get_from_cache(key, end):
    global last_timestamp

    if key in cache.keys():
        if valid_timestamp_for_get(last_timestamp[key], end):
            log("INFO:: Get from Cache: {}".format(key))
            return cache[key]
        else:
            return None

def add_to_cache(key, data, end):
    global last_timestamp

    if last_timestamp[key] is not None:
        if(valid_timestamp_for_add(last_timestamp[key], end)):
            log("INFO:: Add to Cache: {}".format(key))
            last_timestamp[key] = end
            cache[key] = data
    else:
        log("INFO:: Add to Cache: {}".format(key))
        last_timestamp[key] = end
        cache[key] = data
