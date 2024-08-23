import json
from datetime import datetime
from time import time, sleep

import sqlalchemy as sql
import sqlalchemy.exc as sql_exec
import pandas as pd
from sqlalchemy import insert, select
from db_config import engine_str, n_tbl_opt_straddle, remote_engine_str
from common import logger, today, threshold_limit, trading_time_range
# from remote_db_config import engine_str
from db_ops import insert_data_df

execute_retry = True
# db_name = 'data_analytics'
# pg_user = 'postgres'
# pg_pass = 'Vivek001'
# pg_host = '172.16.47.54' # or pg_host = 'localhost'
# pg_port = '5432'
# engine_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5432/{db_name}" #use

pool = sql.create_engine(engine_str, pool_size=10, max_overflow=5, pool_recycle=67, pool_timeout=30, echo=None)
# conn = pool.connect()
remote_pool = sql.create_engine(remote_engine_str, pool_size=10, max_overflow=5, pool_recycle=67, pool_timeout=30, echo=None)
# remote_conn = remote_pool.connect()

def get_master():
    st = datetime.now()
    query = f"""
        SELECT expiry, symbol
        FROM (
            SELECT DISTINCT expiry, symbol
            FROM xts_master
            WHERE symbol IN ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
            AND (
                expiry >= current_date
                AND (
                    (EXTRACT(month FROM expiry) = EXTRACT(month FROM current_date)
                     AND EXTRACT(year FROM expiry) = EXTRACT(year FROM current_date))
                    OR
                    (EXTRACT(month FROM expiry) = mod((EXTRACT(month FROM current_date) + 1), 12)
                     AND EXTRACT(year FROM expiry) = EXTRACT(year FROM current_date) + (CASE WHEN EXTRACT(month FROM current_date) = 12 THEN 1 ELSE 0 END))
                    OR
                    (EXTRACT(month FROM expiry) = mod((EXTRACT(month FROM current_date) + 2), 12)
                     AND EXTRACT(year FROM expiry) = EXTRACT(year FROM current_date) + (CASE WHEN EXTRACT(month FROM current_date) >= 11 THEN 1 ELSE 0 END))
                )
            )
        ) AS subquery
        ORDER BY expiry
    """
    # df1 = read_sql_df(query)
    with pool.connect() as conn:
        df1 = pd.read_sql(query, conn)

    logger.info(f'Master file read from db in {(datetime.now() - st).total_seconds()} seconds')
    return df1

def check_fill_data(extracted_time_range):
    st = datetime.now()
    for each_time in extracted_time_range:
        query = f"""
                SELECT "timestamp"::timestamp at time zone 'Asia/Kolkata' as ts, underlying, expiry, strike, call, put, spot, call_price, put_price, call_oi, put_oi, call_iv, put_iv, combined_premium, combined_iv, otm_iv, minima
                FROM {n_tbl_opt_straddle}
                WHERE minima='true' 
                and "timestamp"::timestamp='{each_time}'
                and call_oi > '{threshold_limit}'
                and put_oi > '{threshold_limit}'
                and call_iv is not null
                and put_iv is not null;
            """
        # df1 = read_sql_df(query)
        with pool.connect() as conn:
            df1 = pd.read_sql(query, conn)
            if len(df1) == 0:
                fill = True
            else:
                fill = False

        if fill:
            query = f"""
                        SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, underlying, expiry, strike, call, put, spot, call_price, put_price, call_oi, put_oi, call_iv, put_iv, combined_premium, combined_iv, otm_iv, minima
                        FROM {n_tbl_opt_straddle}
                        WHERE "timestamp"::timestamp='{each_time}'
                        and call_oi > '{threshold_limit}'
                        and put_oi > '{threshold_limit}'
                        and combined_premium is not null
                    """
            with remote_pool.connect() as remote_conn:
                remote_db_data = pd.read_sql(query, remote_conn)
                remote_db_data.rename(columns={'ts': 'timestamp'}, inplace=True)
                logger.info(f'remote_db_data at {each_time} is \n{remote_db_data}')
            # with pool.connect() as conn:
                # ins = insert(n_tbl_opt_straddle).values(
                #     timestamp=each_time,
                #     underlying=symbol,
                #     expiry=expiry,
                #     strike=remote_db_data['strike'].values[0],
                #     call=remote_db_data['call'].values[0],
                #     put=remote_db_data['put'].values[0],
                #     spot=remote_db_data['spot'].values[0],
                #     call_price=remote_db_data['call_price'].values[0],
                #     put_price=remote_db_data['put_price'].values[0],
                #     call_oi = remote_db_data['call_oi'].values[0],
                #     put_oi=remote_db_data['put_oi'].values[0],
                #     call_iv=remote_db_data['call_iv'].values[0],
                #     put_iv=remote_db_data['put_iv'].values[0],
                #     combined_premium=remote_db_data['combined_premium'].values[0],
                #     combined_iv=remote_db_data['combined_iv'].values[0],
                #     otm_iv=remote_db_data['otm_iv'].values[0],
                #     minima = remote_db_data['minima'].values[0]
                # )
                # conn.execute(ins)
            res = insert_data_df(n_tbl_opt_straddle, remote_db_data)
            if res:
                logger.info(f'Fill data inserted from remote into local db at {each_time}')

        # logger.info(f'Fill data check from db in {(datetime.now() - st).total_seconds()} seconds')
        # return df1
        # print(f'\ndf1 at {each_time} is \n {df1} \n and type is {type(df1)} and len df is {len(df1)}')
        # if len(df1):
        #     print(f'\n and spot is {df1["spot"].values[0]}')
        # sleep(5)

# master_df = get_master()
# print(master_df)
current_minute = pd.Timestamp.now().replace(second=0, microsecond=0, tzinfo=pytz.timezone('Asia/Kolkata'))
#
loc = trading_time_range.index(current_minute)
extracted_time_range = trading_time_range[:loc+1]
if start_time <=
check_fill_data(extracted_time_range)
# # print(f'res is \n {res}')