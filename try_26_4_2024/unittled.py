from datetime import date, datetime, time
import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
# from data_analytics.common import IST, yesterday, today, logger
# from data_analytics.contracts import get_req_contracts
# from data_analytics.db_ops import DBHandler
# from data_analytics.db_config import n_tbl_opt_straddle
# from data_analytics.db_ops import pool

import json
from datetime import datetime
from time import time, sleep
import sqlalchemy as sql
import sqlalchemy.exc as sql_exec
import pandas as pd
from sqlalchemy import insert, select

import pytz
from dateutil import relativedelta
from outside_common import logger, IST, yesterday, today, engine_str, root_dir
import os
import csv
# --------------------------------------------------------------------------------------------------------------------------

pool = sql.create_engine(engine_str, pool_size=10, max_overflow=5, pool_recycle=67, pool_timeout=30, echo=None)
n_tbl_opt_straddle = 'opt_straddle'

print(f'\n today is {today}, yesterday was {yesterday}')

def get_straddle_minima(symbol, expiry, start_from=today):
    query = f"""
        SELECT id, "timestamp" at time zone 'Asia/Kolkata' as ts, underlying, spot, strike, combined_premium, expiry, minima 
        FROM {n_tbl_opt_straddle}
        WHERE underlying=%(symbol)s and expiry=%(expiry)s and minima=true and "timestamp">='{start_from}';
    """
    df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
    return df
# id, timestamp, underlying, expiry = today, strike, spot, combined_premium, minima = true

def read_sql_df(query, params=None, commit=False):
    st = time()
    logger.debug(f"Reading query..{query[:int(len(query)*0.25)]}...")
    # engine = sql.create_engine(engine_str)
    conn = pool.connect()
    df = pd.read_sql(query, conn, params=params)
    if commit:
        conn.execute('commit')
    conn.close()
    # engine.dispose()
    # logger.debug(f'Data read in {time() - st} secs')
    return df

def fetch_straddle_minima(symbol: str, expiry: date, st_cnt: int=None, interval: int= 5, cont: bool = False):
    if cont:
        df = get_straddle_minima(symbol, expiry, start_from=yesterday)
        print('\n returned df1 is \n', df.head())
        df['prev'] = df['ts'] < today
        print('\n returned df1 after oprn is \n', df.head())
    else:
        df = get_straddle_minima(symbol, expiry)
        print('\n returned df2 is \n', df.head())
        df['prev'] = False
    # return _straddle_response(df, count=st_cnt, interval=interval)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print('\n pdmaxdf \n',df)
    # pd.set_option('display.max_rows', 500)
    # pd.set_option('display.max_columns', 500)
    # pd.set_option('display.width', 1000)
    # print('\n pdmaxdf \n', df)
    file_path = os.path.join(root_dir, 'fetch_straddle.csv')
    # if os.path.exists(file_path):
    #     df1 = pd.read_csv(file_path)
    #     df1.append(df)
    # else:
    #     df1 = df



    # df1 = pd.DataFrame
    # if not os.path.exists(file_path):
    #     with open(file_path, 'w') as f:
    #         df1 = pd.read_csv(file_path)
    # df1 = df1.append(df, ignore_index=True)
    # df1.to_csv(file_path)
    # print('\n df to csv successful')

symbol = {'NIFTY': '2024-04-25', 'BANKNIFTY': '2024-04-30'}
# symbol = 'BANKNIFTY'
# expiry = '2024-04-30'

# fetch_straddle_minima(symbol, expiry)

# def get_symbols(self):
#     if self.symbol_expiry_map is None:
#         ins_df, tokens, token_xref = get_req_contracts()
#         ins_df['expiry'] = ins_df['expiry'].dt.strftime('%Y-%m-%d')
#         agg = ins_df[ins_df['instrument_type'].isin(['CE', 'PE'])].groupby(['name'], as_index=False).agg({'expiry': set, 'tradingsymbol': 'count'})
#         agg['expiry'] = agg['expiry'].apply(lambda x: sorted(list(x)))
#         self.symbol_expiry_map = agg.to_dict('records')

# for symbol, expiry in symbol.items():
#     print(f'\n sent args are {symbol} {expiry}')
#     fetch_straddle_minima(symbol, datetime.strptime(expiry, '%Y-%m-%d'))

cur_date = datetime.now(IST).replace(microsecond=0, tzinfo=None)
print('\n cur date is',cur_date)
start = cur_date.replace(hour = 9, minute = 30, second=0, microsecond=0)
end = cur_date.replace(hour=15, minute=30, second=0, microsecond=0)
print(f'\n start time is {start} and end time is {end}')
