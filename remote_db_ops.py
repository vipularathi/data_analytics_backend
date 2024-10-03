import json
from datetime import datetime
from time import time, sleep
import pytz
import sqlalchemy as sql
import sqlalchemy.exc as sql_exec
import pandas as pd
from sqlalchemy import insert, select
from concurrent.futures import ThreadPoolExecutor

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker

from db_config import engine_str, n_tbl_opt_straddle, remote_engine_str
from common import logger, today, threshold_limit, trading_time_range, start_time, end_time
# from remote_db_config import engine_str
from db_ops import insert_data_df

execute_retry = True
db_name = f'data_arathi_9_apr_2024'
pg_user = 'postgres'
pg_pass = 'root'
pg_host = '172.16.47.81'
pg_port = '5432'

remote_db_name = f'data_arathi_9_apr_2024'
remote_pg_user = 'postgres'
remote_pg_pass = 'Vivek001'
remote_pg_host = '172.16.47.54'
remote_pg_port = '5432'

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
# # #first try(working) tte>20sec
# # def check_fill_data(extracted_time_range):
# #     st = datetime.now()
# #     for each_time in extracted_time_range:
# #         query = f"""
# #                 SELECT id, "timestamp" at time zone 'Asia/Kolkata' as ts, underlying, expiry, strike, call, put, spot, call_price, put_price, call_oi, put_oi, call_iv, put_iv, combined_premium, combined_iv, otm_iv, minima
# #                 FROM {n_tbl_opt_straddle}
# #                 WHERE minima='true'
# #                 and "timestamp"::timestamp='{each_time}'
# #                 and call_oi > '{threshold_limit}'
# #                 and put_oi > '{threshold_limit}'
# #                 and call_iv is not null
# #                 and put_iv is not null;
# #             """
# #         # df1 = read_sql_df(query)
# #         with pool.connect() as conn:
# #             df1 = pd.read_sql(query, conn)
# #             if len(df1) == 0:
# #                 fill = True
# #             else:
# #                 fill = False
# #         if fill:
# #             query = f"""
# #                         SELECT id, "timestamp" at time zone 'Asia/Kolkata' as ts, underlying, expiry, strike, call, put, spot, call_price, put_price, call_oi, put_oi, call_iv, put_iv, combined_premium, combined_iv, otm_iv, minima
# #                         FROM {n_tbl_opt_straddle}
# #                         WHERE "timestamp"::timestamp='{each_time}'
# #                         and call_oi > '{threshold_limit}'
# #                         and put_oi > '{threshold_limit}'
# #                         and combined_premium is not null
# #                     """
# #             with remote_pool.connect() as remote_conn:
# #                 remote_db_data = pd.read_sql(query, remote_conn)
# #                 remote_db_data.rename(columns={'ts': 'timestamp'}, inplace=True)
# #                 logger.info(f'remote_db_data at {each_time} is \n{remote_db_data}')
# #             res = insert_data_df(n_tbl_opt_straddle, remote_db_data)
# #             if res:
# #                 logger.info(f'Fill data inserted from remote into local db at {each_time}')
#
# # # #3rd try(not working)
# # # Connection pools for async access
# # # local_engine = create_async_engine(f'postgresql+asyncpg://{pg_user}:{pg_pass}@{pg_host}/{db_name}', echo=False, pool_size=20)
# # # remote_engine = create_async_engine(f'postgresql+asyncpg://{remote_pg_user}:{remote_pg_pass}@{remote_pg_host}/{remote_db_name}', echo=False, pool_size=20)
# # # AsyncSessionLocal = sessionmaker(local_engine, expire_on_commit=False, class_=AsyncSession)
# # # AsyncSessionRemote = sessionmaker(remote_engine, expire_on_commit=False, class_=AsyncSession)
# # #
# # # async def fetch_local_data(session, times_str):
# # #     query = f"""
# # #             SELECT id, "timestamp" at time zone 'Asia/Kolkata' as ts, underlying, expiry, strike, call, put, spot, call_price, put_price, call_oi, put_oi, call_iv, put_iv, combined_premium, combined_iv, otm_iv, minima
# # #             FROM {n_tbl_opt_straddle}
# # #             WHERE minima='true'
# # #             AND "timestamp" IN ({times_str})
# # #             AND call_oi > {threshold_limit}
# # #             AND put_oi > {threshold_limit}
# # #             AND (call_iv IS NOT NULL AND put_iv IS NOT NULL OR combined_premium IS NOT NULL);
# # #         """
# # #     result = await session.execute(query)
# # #     return pd.DataFrame(result.fetchall(), columns=result.keys())
# # #
# # # async def fetch_remote_data(session, missing_times_str):
# # #     query = f"""
# # #             SELECT id, "timestamp" at time zone 'Asia/Kolkata' as ts, underlying, expiry, strike, call, put, spot, call_price, put_price, call_oi, put_oi, call_iv, put_iv, combined_premium, combined_iv, otm_iv, minima
# # #             FROM {n_tbl_opt_straddle}
# # #             WHERE "timestamp" IN ({missing_times_str})
# # #             AND call_oi > {threshold_limit}
# # #             AND put_oi > {threshold_limit}
# # #             AND combined_premium IS NOT NULL;
# # #         """
# # #     result = await session.execute(query)
# # #     return pd.DataFrame(result.fetchall(), columns=result.keys())
# # #
# # # async def check_and_fill_data_async(extracted_time_range):
# # #     times_str = ', '.join([f"'{time}'" for time in extracted_time_range])
# # #
# # #     async with AsyncSessionLocal() as local_session:
# # #         local_result = await fetch_local_data(local_session, times_str)
# # #         local_df = pd.DataFrame(local_result.fetchall())
# # #
# # #     missing_times = set(extracted_time_range) - set(local_df['ts'].dt.floor('T'))
# # #
# # #     if missing_times:
# # #         missing_times_str = ', '.join([f"'{time}'" for time in missing_times])
# # #         async with AsyncSessionRemote() as remote_session:
# # #             remote_result = await fetch_remote_data(remote_session, missing_times_str)
# # #             remote_db_data = pd.DataFrame(remote_result.fetchall())
# # #             remote_db_data.rename(columns={'ts': 'timestamp'}, inplace=True)
# # #             logger.info(f'remote_db_data for missing times is \n{remote_db_data}')
# # #
# # #         res = insert_data_df(n_tbl_opt_straddle, remote_db_data)
# # #         if res:
# # #             logger.info(f'Fill data inserted from remote into local db for missing times')
# # #
# # # async def check_fill_data_parallel(extracted_time_ranges):
# # #     tasks = [check_and_fill_data_async(extracted_time_range) for extracted_time_range in extracted_time_ranges]
# # #     await asyncio.gather(*tasks)
# #2nd try(using multi-processing)(working) tte>20sec
def check_fill_data(extracted_time_range):
    time_str = ', '.join([f"'{time}'" for time in extracted_time_range])
    for each_time in extracted_time_range:
        query = f"""
                SELECT id, "timestamp" at time zone 'Asia/Kolkata' as ts, underlying, expiry, strike, call, put, spot, call_price, put_price, call_oi, put_oi, call_iv, put_iv, combined_premium, combined_iv, otm_iv, minima
                FROM {n_tbl_opt_straddle}
                WHERE minima='true'
                and "timestamp"::timestamp in ({time_str})
                and call_oi > '{threshold_limit}'
                and put_oi > '{threshold_limit}'
                and (
                    call_iv is not null
                    and put_iv is not null
                    or combined_premium is not null
                );
            """
        # df1 = read_sql_df(query)
        with pool.connect() as conn:
            df1 = pd.read_sql(query, conn)
        local_missing_time = set(extracted_time_range) - set(df1['ts'].dt.floor('T'))

        if local_missing_time:
            missing_time_str = ', '.join([f"'{time}'" for time in local_missing_time])
            query = f"""
                        SELECT id, "timestamp" at time zone 'Asia/Kolkata' as ts, underlying, expiry, strike, call, put, spot, call_price, put_price, call_oi, put_oi, call_iv, put_iv, combined_premium, combined_iv, otm_iv, minima
                        FROM {n_tbl_opt_straddle}
                        WHERE "timestamp"::timestamp in ({missing_time_str})
                        and call_oi > '{threshold_limit}'
                        and put_oi > '{threshold_limit}'
                        and combined_premium is not null
                    """
            with remote_pool.connect() as remote_conn:
                remote_db_data = pd.read_sql(query, remote_conn)
                remote_db_data.rename(columns={'ts': 'timestamp'}, inplace=True)
                logger.info(f'remote_db_data at {each_time} is \n{remote_db_data}')
            res = insert_data_df(n_tbl_opt_straddle, remote_db_data)
            if res:
                logger.info(f'Fill data inserted from remote into local db at {each_time}')
def check_fill_data_parallel(trading_time_range):
    with ThreadPoolExecutor() as executor:
        executor.map(check_fill_data, trading_time_range)

master_df = get_master()
# print(master_df)
# current_minute = pd.Timestamp.now().replace(second=0, microsecond=0)
#
#
# if start_time <= current_minute <= end_time:
#     loc = trading_time_range.index(current_minute)
#     extracted_time_range = trading_time_range[:loc+1]
#     check_fill_data(extracted_time_range)
# elif current_minute > end_time:
#     check_fill_data(trading_time_range)
# # print(f'res is \n {res}')