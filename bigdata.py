import pandas as pd
import json
from db import conn, reconnect
from utils import*
from cache import*

default_period = '1min'
default_status = 2

select_between_timestamps_query = "SELECT createdAt, volume, price, token_id FROM tradeHistory WHERE status = %(default_status)s AND token_id = %(token_id)s AND createdAt BETWEEN %(begin)s AND %(end)s"
select_after_timestamp_query = "SELECT createdAt, volume, price, token_id FROM tradeHistory WHERE status = %(default_status)s AND token_id = %(token_id)s AND createdAt < %(end)s limit 1"

def read_from_sql(token_id, begin, end, query):
    params = {"token_id":token_id, "begin":parse_timestamp(begin), "end":parse_timestamp(end), "default_status":default_status}
    df = pd.read_sql(con = conn,
                    sql = query,
                    params = params,
                    parse_dates = True,
                    index_col = 'createdAt')
    conn.commit()
    return df

def safe_read_from_sql(token_id, begin, end, query):
    if not conn.is_connected():
        reconnect()
        return pd.DataFrame()
    else:
        return read_from_sql(token_id, begin, end, query)

def fill_ohlcv_gaps(df):
    closes = df.close.fillna(method='pad')
    df = df.apply(lambda x: x.fillna(closes))
    return df

def fix_volume(df):
    df['volume'] = df['volume'] / 10**18
    return df

def convert_to_ohlcv(df, period):
    df.head()
    ohlcv = df.resample(period).agg({'price': 'ohlc', 'volume': 'sum'})
    ohlcv.columns = ohlcv.columns.droplevel(0)
    ohlcv.index.names = ['time']
    ohlcv = fill_ohlcv_gaps(ohlcv)
    ohlcv = fix_volume(ohlcv)
    jdata = json.loads(ohlcv.reset_index().to_json(orient = 'records'))
    result = json.dumps(jdata, indent = 2)
    return result

def build_data(token_id, begin, end, period):
    global select_between_timestamps_query
    key = None
    if str(period) == default_period:
        key = (token_id, period)
        ohlcv = get_from_cache(key, end)

        if ohlcv is not None:
           return ohlcv

    df = safe_read_from_sql(token_id, begin, end, select_between_timestamps_query)
    if not df.empty:
        ohlcv = convert_to_ohlcv(df, period)
        log("INFO:: Get fresh data (no cache): {}".format(key))

        if key is not None:
           add_to_cache(key, ohlcv, end)

        return ohlcv
    else:
        df = safe_read_from_sql(token_id, begin, end, select_after_timestamp_query)
        if not df.empty: 
            beginIndexCol = parse_timestamp(begin)
            endIndexCol = parse_timestamp(end)

            df.at[beginIndexCol, 'volume'] = df.at[df.index.values[0], 'volume']
            df.at[beginIndexCol, 'price'] = df.at[df.index.values[0], 'price']
            df.at[beginIndexCol, 'token_id'] = df.at[df.index.values[0], 'token_id']

            df.at[endIndexCol, 'volume'] = df.at[df.index.values[0], 'volume']
            df.at[endIndexCol, 'price'] = df.at[df.index.values[0], 'price']
            df.at[endIndexCol, 'token_id'] = df.at[df.index.values[0], 'token_id']

            df = df.iloc[1:] #exclude first row (i.e. the first found record closest to the end date)
            df.index = pd.to_datetime(df.index)
            df = convert_to_ohlcv(df, period)

            if key is not None:
                add_to_cache(key, ohlcv, end)

            return df
        else:
            return []
