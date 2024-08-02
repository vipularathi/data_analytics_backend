# import json
# from datetime import datetime, timedelta
# from time import time, sleep
#
# import sqlalchemy as sql
# import sqlalchemy.exc as sql_exec
# import pandas as pd
# from sqlalchemy import insert, select
#
# from common import logger, today
# from db_config import engine_str, use_sqlite, s_tbl_snap, n_tbl_snap, s_tbl_opt_greeks, n_tbl_opt_greeks, s_tbl_opt_straddle, \
#     n_tbl_opt_straddle, n_tbl_master, s_tbl_master
#
# execute_retry = True
# pool = sql.create_engine(engine_str, pool_size=10, max_overflow=5, pool_recycle=67, pool_timeout=30, echo=None)
#
# threshold_limit = 10000
#
# def insert_data(table: sql.Table, dict_data, engine_address=None, multi=False, ignore=False, truncate=False, retry=1, wait_period=5):
#     """
#     This is used to insert the data in dict format into the table
#     :param table: SQLAlchemy Table Object
#     :param dict_data: Data to be inserted
#     :param engine_address: Define a custom engine string. Use default if None provided.
#     :param multi: Whether to use multi query or not
#     :param ignore: Use Insert Ignore while insertion
#     :param truncate: Truncate table before insertion
#     :param retry: Insert Retry Number
#     :param wait_period: Time in seconds for retry
#     :return: None
#     """
#     st = time()
#     logger.debug(f'Data Insertion started for {table.name}')
#     logger.info(f'Data Insertion started for {table.name}')
#     # engine_con_str = engine_address if engine_address is not None else engine_str
#     # engine = sql.create_engine(engine_con_str)
#     ins = table.insert()
#     if ignore:
#         if use_sqlite:
#             ignore_clause = 'IGNORE' if not use_sqlite else 'OR IGNORE'
#             ins = table.insert().prefix_with(ignore_clause)
#         else:
#             # Considering Postgres
#             from sqlalchemy.dialects.postgresql import insert
#             # ins = table.insert().on_conflict_do_nothing()  # Not available
#             ins = insert(table).on_conflict_do_nothing()
#
#     with pool.connect() as conn:
#         if truncate:
#             conn.execute(f'TRUNCATE TABLE {table.name}')
#         try:
#             conn.execute(ins, dict_data, multi=multi)
#         except sql_exec.OperationalError as e:
#             if retry > 0:
#                 logger.info(f"Error for {table.name}: {e}")
#                 logger.info(f'Retrying to insert data in {table.name} after {wait_period} seconds')
#                 sleep(wait_period)
#                 insert_data(table=table, dict_data=dict_data, engine_address=engine_address, multi=multi, ignore=ignore,
#                             truncate=truncate, retry=retry-1, wait_period=wait_period)
#             else:
#                 logger.error(f"Error for {table.name} insertion: {e}", escalate=True)
#         conn.close()
#
#     logger.debug(f"Data Inserted in {table.name} in: {time() - st} secs")
#     logger.info(f"Data Inserted in {table.name} in: {time() - st} secs")
#
#
# def insert_data_df(table, data: pd.DataFrame, truncate=False, master = False):
#     conn = pool.connect()
#     if truncate:
#         conn.execute(f'TRUNCATE TABLE {table.name}')
#     if master:
#         st = time()
#         logger.info(f'Data insertion started for master')
#         response = data.to_sql(table, con=conn, if_exists='replace', index=False)
#         # df.to_sql(tablename, conn, if_exists='replace', index=False)
#         logger.info(f'Data insertion completed for master in: {time() - st} secs')
#         return response
#     else:
#         response = data.to_sql(table.name, con=conn, if_exists='append', index=False, method='multi')
#     conn.close()
#     return response
#
#
# def execute_query(query, retry=2, wait_period=5, params=None):
#     if params is None:
#         params = {}
#     st = time()
#     short_query = query[:int(len(query)*0.25)] if type(query) is str else ''
#     # logger.debug(f'Executing query...{short_query}...')
#     # engine = sql.create_engine(engine_str)
#     try:
#         with pool.connect() as conn:
#             try:
#                 result = conn.execute(query, params)
#             except:
#                 result = conn.execute(sql.text(query), params).mappings() #for login
#             # conn.execute(ins, dict_data, multi=multi)
#             conn.close()
#     except sql_exec.OperationalError as e:
#         if retry > 0:
#             logger.info(f"Error for Query {short_query}: {e}")
#             logger.info(f'Retrying to execute query {short_query} after {wait_period} seconds')
#             sleep(wait_period)
#             result = execute_query(query=query, retry=retry-1, wait_period=wait_period)
#         else:
#             logger.error(f"Error for Query {short_query}: {e}", escalate=True)
#
#     # logger.debug(f"Time taken to execute query: {time() - st} secs")
#     return result
#
#
# def read_sql_df(query, params=None, commit=False):
#     st = time()
#     # logger.debug(f"Reading query..{query[:int(len(query)*0.25)]}...")
#     logger.info(f"Reading query..{query[:int(len(query)*0.25)]}...")
#     # engine = sql.create_engine(engine_str)
#     conn = pool.connect()
#     df = pd.read_sql(query, conn, params=params)
#     if commit:
#         conn.execute('commit')
#     conn.close()
#     # engine.dispose()
#     # logger.debug(f'Data read in {time() - st} secs')
#     logger.info(f'Data read in {time() - st} secs')
#     return df
#
#
# def calculate_table_data(df):
#     df1 = df.copy()
#     live = (df1['combined_premium'].iloc[-1]).round(2)
#     max_straddle = (df1['combined_premium'].max()).round(2)
#     min_straddle = (df1['combined_premium'].min()).round(2)
#     live_min = (live - min_straddle).round(2)
#     max_live = (max_straddle - live).round(2)
#     ret_dict = [{
#         'Live':live,
#         'Live-Min':live_min,
#         'Max-Live':max_live,
#         'Max':max_straddle,
#         'Min':min_straddle
#     }]
#     logger.info(f'\nret_dict is {ret_dict}')
#     # ret_df = pd.DataFrame.from_dict(ret_dict)
#     # dict_to_json = [{i:ret_dict[i]} for i in ret_dict]
#     # logger.info(f'\n dict_to_json is {dict_to_json}')
#     return ret_dict
#
# class DBHandler:
#
#     """
#     Meant to handle Signal related stuff only.
#     """
#
#     @classmethod
#     def check_user_exist(cls, email):
#         # query = '''SELECT * FROM users;'''
#         # response = execute_query(query)
#
#         query = '''SELECT email, password FROM chart_users WHERE email = :email'''
#         response = execute_query(query, params={"email": email})
#         data = response.fetchone()
#
#         if data is None:
#             return False, False
#         else:
#             return True, data
#
#     @classmethod
#     def build_users_params(cls, users: list):
#         users_in = {f"users_{_i}": _u for _i, _u in enumerate(users)}
#         params_in = ",".join([f"%({i})s" for i in users_in.keys()])
#         return users_in, params_in
#
#     @classmethod
#     def insert_snap_data(cls, db_data: list[dict]):
#         logger.info('insert_snap>insert_data')
#         insert_data(s_tbl_snap, db_data, ignore=True)
#
#     @classmethod
#     def get_snap_data(cls, ts: datetime):
#         query = f"""
#             SELECT * FROM {n_tbl_snap} WHERE "timestamp"='{ts}'
#         """
#         result = execute_query(query, params={'ts': ts})
#         response = result.fetchall()
#         return response
#
#     @classmethod
#     def insert_opt_greeks(cls, db_data: list[dict]):
#         insert_data(s_tbl_opt_greeks, db_data, ignore=True)
#
#     @classmethod
#     def insert_opt_straddle(cls, db_data: list[dict]):
#         insert_data(s_tbl_opt_straddle, db_data, ignore=True)
#
#     @classmethod
#     def get_straddle_minima(cls, symbol, expiry, start_from=today, table: bool=False):
#         logger.info('get_straddle_minima')
#         query = f"""
#             SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv
#             FROM {n_tbl_opt_straddle}
#             WHERE underlying=%(symbol)s and expiry=%(expiry)s and minima=true and timestamp>='{start_from}' and call_oi > {threshold_limit} and put_oi > {threshold_limit};
#         """
#         df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry, 'threshold_limit': threshold_limit})
#
#         if table:
#             logger.info(f'\ndf made from read_sql_df is \n{df.head()}')
#             logger.info(f"\nLive is {df['combined_premium'].iloc[-1]} max = {df['combined_premium'].max()} \t min is {df['combined_premium'].min()}")
#             logger.info('\n df sent for trucation')
#             table_df = calculate_table_data(df)
#             return table_df
#         else:
#             return df
#         # return df
#
#
#     @classmethod
#     def get_straddle_minima_table(cls, symbol, expiry, start_from=today):
#         query = f"""
#             SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, combined_premium
#             FROM {n_tbl_opt_straddle}
#             WHERE underlying=%(symbol)s and expiry=%(expiry)s and minima=true and "timestamp">='{start_from}';
#         """
#         df = read_sql_df(query, params={'symbol':symbol, 'expiry':expiry})
#         logger.info(f'\ndf made from read_sql_df is \n{df.head()}')
#         logger.info(f"\nLive is {df['combined_premium'].iloc[-1]} max = {df['combined_premium'].max()} \t min is {df['combined_premium'].min()}")
#         logger.info('\n df sent for trucation')
#         table_dict = calculate_table_data(df)
#         return table_dict
#
#     # @classmethod
#     # def get_straddle_iv_data(cls, symbol, expiry, start_from=today):
#     #     query = f"""
#     #             SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv, minima
#     #             FROM {n_tbl_opt_straddle}
#     #             WHERE underlying=%(symbol)s and expiry=%(expiry)s and "timestamp">='{start_from}';
#     #         """
#     #     df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
#     #     return df
#
#     @classmethod
#     def get_straddle_iv_data(cls, symbol, expiry, start_from=today):
#         query = f"""
#                     SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv, minima
#                     FROM {n_tbl_opt_straddle}
#                     WHERE underlying=%(symbol)s and expiry=%(expiry)s and timestamp>='{start_from}';
#                 """
#         df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
#         return df
#
#     @classmethod
#     def get_credentials(cls):
#         query = f"""
#                 SELECT * FROM {s_tbl_creds} WHERE status = 'active'
#             """
#         df = read_sql_df(query)
#         return df
#
#     @classmethod
#     def insert_credentials(cls, db_data):
#         insert_data(s_tbl_creds, db_data, ignore=True)
#
#     @classmethod
#     def update_credentials(cls, appkey, new_token):
#         update_query = f"""
#                         UPDATE creds SET token = '{new_token}' WHERE appkey = '{appkey}'
#                     """
#         result = execute_query(update_query)
#         logger.info(result)
#
#     @classmethod
#     def delete_old_data(cls):
#         table_list = [n_tbl_snap, n_tbl_opt_greeks, n_tbl_opt_straddle]
#         a = (today - timedelta(days=30)).date().strftime("%Y-%m-%d")
#         # b = pd.to_datetime('2024-05-20').date().strftime('%Y-%m-%d')
#         # c = '2024-05-20'
#         for each_table in table_list:
#             delete_query = f"""
#                         DELETE FROM {each_table} WHERE "timestamp"<:cutoff_date
#                     """
#             result = execute_query(delete_query, params={'cutoff_date': a})
#             logger.info(result)
#         return True
#
# # res = DBHandler.delete_old_data()
# # logger.info(res)


# import json
# from datetime import datetime, timedelta
# from time import time, sleep
#
# import sqlalchemy as sql
# import sqlalchemy.exc as sql_exec
# import pandas as pd
# from sqlalchemy import insert, select
#
# from common import logger, today
# from db_config import engine_str, use_sqlite, s_tbl_snap, n_tbl_snap, s_tbl_opt_greeks, n_tbl_opt_greeks, s_tbl_opt_straddle, \
#     n_tbl_opt_straddle, n_tbl_master, s_tbl_master
#
# execute_retry = True
# pool = sql.create_engine(engine_str, pool_size=10, max_overflow=5, pool_recycle=67, pool_timeout=30, echo=None)
#
# threshold_limit = 10000
#
# def insert_data(table: sql.Table, dict_data, engine_address=None, multi=False, ignore=False, truncate=False, retry=1, wait_period=5):
#     """
#     This is used to insert the data in dict format into the table
#     :param table: SQLAlchemy Table Object
#     :param dict_data: Data to be inserted
#     :param engine_address: Define a custom engine string. Use default if None provided.
#     :param multi: Whether to use multi query or not
#     :param ignore: Use Insert Ignore while insertion
#     :param truncate: Truncate table before insertion
#     :param retry: Insert Retry Number
#     :param wait_period: Time in seconds for retry
#     :return: None
#     """
#     st = time()
#     logger.debug(f'Data Insertion started for {table.name}')
#     # # logger.info(f'Data Insertion started for {table.name}')
#     # engine_con_str = engine_address if engine_address is not None else engine_str
#     # engine = sql.create_engine(engine_con_str)
#     ins = table.insert()
#     if ignore:
#         if use_sqlite:
#             ignore_clause = 'IGNORE' if not use_sqlite else 'OR IGNORE'
#             ins = table.insert().prefix_with(ignore_clause)
#         else:
#             # Considering Postgres
#             from sqlalchemy.dialects.postgresql import insert
#             # ins = table.insert().on_conflict_do_nothing()  # Not available
#             ins = insert(table).on_conflict_do_nothing()
#
#     with pool.connect() as conn:
#         if truncate:
#             conn.execute(f'TRUNCATE TABLE {table.name}')
#         try:
#             conn.execute(ins, dict_data, multi=multi)
#         except sql_exec.OperationalError as e:
#             if retry > 0:
#                 # # logger.info(f"Error for {table.name}: {e}")
#                 # # logger.info(f'Retrying to insert data in {table.name} after {wait_period} seconds')
#                 sleep(wait_period)
#                 insert_data(table=table, dict_data=dict_data, engine_address=engine_address, multi=multi, ignore=ignore,
#                             truncate=truncate, retry=retry-1, wait_period=wait_period)
#             else:
#                 logger.error(f"Error for {table.name} insertion: {e}", escalate=True)
#         conn.close()
#
#     logger.debug(f"Data Inserted in {table.name} in: {time() - st} secs")
#     # # logger.info(f"Data Inserted in {table.name} in: {time() - st} secs")
#
#
# def insert_data_df(table, data: pd.DataFrame, truncate=False, master = False):
#     conn = pool.connect()
#     if truncate:
#         conn.execute(f'TRUNCATE TABLE {table.name}')
#     if master:
#         st = time()
#         # # logger.info(f'Data insertion started for master')
#         response = data.to_sql(table, con=conn, if_exists='replace', index=False)
#         # df.to_sql(tablename, conn, if_exists='replace', index=False)
#         # # logger.info(f'Data insertion completed for master in: {time() - st} secs')
#         return response
#     else:
#         response = data.to_sql(table.name, con=conn, if_exists='append', index=False, method='multi')
#     conn.close()
#     return response
#
#
# def execute_query(query, retry=2, wait_period=5, params=None):
#     if params is None:
#         params = {}
#     st = time()
#     short_query = query[:int(len(query)*0.25)] if type(query) is str else ''
#     # logger.debug(f'Executing query...{short_query}...')
#     # engine = sql.create_engine(engine_str)
#     try:
#         with pool.connect() as conn:
#             try:
#                 result = conn.execute(query, params)
#             except:
#                 result = conn.execute(sql.text(query), params).mappings() #for login
#             # conn.execute(ins, dict_data, multi=multi)
#             conn.close()
#     except sql_exec.OperationalError as e:
#         if retry > 0:
#             # # logger.info(f"Error for Query {short_query}: {e}")
#             # # logger.info(f'Retrying to execute query {short_query} after {wait_period} seconds')
#             sleep(wait_period)
#             result = execute_query(query=query, retry=retry-1, wait_period=wait_period)
#         else:
#             logger.error(f"Error for Query {short_query}: {e}", escalate=True)
#
#     # logger.debug(f"Time taken to execute query: {time() - st} secs")
#     return result
#
#
# def read_sql_df(query, params=None, commit=False):
#     st = time()
#     # logger.debug(f"Reading query..{query[:int(len(query)*0.25)]}...")
#     # # logger.info(f"Reading query..{query[:int(len(query)*0.25)]}...")
#     # engine = sql.create_engine(engine_str)
#     conn = pool.connect()
#     df = pd.read_sql(query, conn, params=params)
#     if commit:
#         conn.execute('commit')
#     conn.close()
#     # engine.dispose()
#     # logger.debug(f'Data read in {time() - st} secs')
#     # # logger.info(f'Data read in {time() - st} secs')
#     return df
#
#
# def calculate_table_data(df):
#     df1 = df.copy()
#     live = (df1['combined_premium'].iloc[-1]).round(2)
#     max_straddle = (df1['combined_premium'].max()).round(2)
#     min_straddle = (df1['combined_premium'].min()).round(2)
#     live_min = (live - min_straddle).round(2)
#     max_live = (max_straddle - live).round(2)
#     ret_dict = [{
#         'Live':live,
#         'Live-Min':live_min,
#         'Max-Live':max_live,
#         'Max':max_straddle,
#         'Min':min_straddle
#     }]
#     # # logger.info(f'\nret_dict is {ret_dict}')
#     # ret_df = pd.DataFrame.from_dict(ret_dict)
#     # dict_to_json = [{i:ret_dict[i]} for i in ret_dict]
#     # # # logger.info(f'\n dict_to_json is {dict_to_json}')
#     return ret_dict
#
# class DBHandler:
#
#     """
#     Meant to handle Signal related stuff only.
#     """
#
#     @classmethod
#     def check_user_exist(cls, email):
#         # query = '''SELECT * FROM users;'''
#         # response = execute_query(query)
#
#         query = '''SELECT email, password FROM chart_users WHERE email = :email'''
#         response = execute_query(query, params={"email": email})
#         data = response.fetchone()
#
#         if data is None:
#             return False, False
#         else:
#             return True, data
#
#     @classmethod
#     def build_users_params(cls, users: list):
#         users_in = {f"users_{_i}": _u for _i, _u in enumerate(users)}
#         params_in = ",".join([f"%({i})s" for i in users_in.keys()])
#         return users_in, params_in
#
#     @classmethod
#     def insert_snap_data(cls, db_data: list[dict]):
#         # # logger.info('insert_snap>insert_data')
#         insert_data(s_tbl_snap, db_data, ignore=True)
#
#     @classmethod
#     def get_snap_data(cls, ts: datetime):
#         query = f"""
#             SELECT * FROM {n_tbl_snap} WHERE "timestamp"='{ts}'
#         """
#         result = execute_query(query, params={'ts': ts})
#         response = result.fetchall()
#         return response
#
#     @classmethod
#     def insert_opt_greeks(cls, db_data: list[dict]):
#         insert_data(s_tbl_opt_greeks, db_data, ignore=True)
#
#     @classmethod
#     def insert_opt_straddle(cls, db_data: list[dict]):
#         insert_data(s_tbl_opt_straddle, db_data, ignore=True)
#
#     @classmethod
#     def get_straddle_minima(cls, symbol, expiry, start_from=today, table: bool=False):
#         # # logger.info('get_straddle_minima')
#         query = f"""
#             SELECT timestamp as ts, spot, strike, combined_premium, combined_iv, otm_iv
#             FROM {n_tbl_opt_straddle}
#             WHERE underlying=%(symbol)s and expiry=%(expiry)s and minima=true and timestamp>='{start_from}';
#         """
#         df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
#
#         if table:
#             # # logger.info(f'\ndf made from read_sql_df is \n{df.head()}')
#             # # logger.info(f"\nLive is {df['combined_premium'].iloc[-1]} max = {df['combined_premium'].max()} \t min is {df['combined_premium'].min()}")
#             # # logger.info('\n df sent for trucation')
#             table_df = calculate_table_data(df)
#             return table_df
#         else:
#             return df
#         # return df
#
#
#     @classmethod
#     def get_straddle_minima_table(cls, symbol, expiry, start_from=today):
#         query = f"""
#             SELECT timestamp as ts, combined_premium
#             FROM {n_tbl_opt_straddle}
#             WHERE underlying=%(symbol)s and expiry=%(expiry)s and minima=true and "timestamp">='{start_from}';
#         """
#         df = read_sql_df(query, params={'symbol':symbol, 'expiry':expiry})
#         # # logger.info(f'\ndf made from read_sql_df is \n{df.head()}')
#         # # logger.info(f"\nLive is {df['combined_premium'].iloc[-1]} max = {df['combined_premium'].max()} \t min is {df['combined_premium'].min()}")
#         # # logger.info('\n df sent for trucation')
#         table_dict = calculate_table_data(df)
#         return table_dict
#
#     # @classmethod
#     # def get_straddle_iv_data(cls, symbol, expiry, start_from=today):
#     #     query = f"""
#     #             SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv, minima
#     #             FROM {n_tbl_opt_straddle}
#     #             WHERE underlying=%(symbol)s and expiry=%(expiry)s and "timestamp">='{start_from}';
#     #         """
#     #     df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
#     #     return df
#
#     @classmethod
#     def get_straddle_iv_data(cls, symbol, expiry, start_from=today):
#         query = f"""
#                     SELECT timestamp as ts, spot, strike, combined_premium, combined_iv, otm_iv, minima
#                     FROM {n_tbl_opt_straddle}
#                     WHERE underlying=%(symbol)s and expiry=%(expiry)s and timestamp>='{start_from}';
#                 """
#         df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
#         return df
#
#     @classmethod
#     def get_credentials(cls):
#         query = f"""
#                 SELECT * FROM {s_tbl_creds} WHERE status = 'active'
#             """
#         df = read_sql_df(query)
#         return df
#
#     @classmethod
#     def insert_credentials(cls, db_data):
#         insert_data(s_tbl_creds, db_data, ignore=True)
#
#     @classmethod
#     def update_credentials(cls, appkey, new_token):
#         update_query = f"""
#                         UPDATE creds SET token = '{new_token}' WHERE appkey = '{appkey}'
#                     """
#         result = execute_query(update_query)
#         # # logger.info(result)
#
#     @classmethod
#     def delete_old_data(cls):
#         table_list = [n_tbl_snap, n_tbl_opt_greeks, n_tbl_opt_straddle]
#         a = (today - timedelta(days=30)).date().strftime("%Y-%m-%d")
#         # b = pd.to_datetime('2024-05-20').date().strftime('%Y-%m-%d')
#         # c = '2024-05-20'
#         for each_table in table_list:
#             delete_query = f"""
#                         DELETE FROM {each_table} WHERE "timestamp"<:cutoff_date
#                     """
#             result = execute_query(delete_query, params={'cutoff_date': a})
#             # # logger.info(result)
#         return True
#
# # res = DBHandler.delete_old_data()
# # # # logger.info(res)

import json
from datetime import datetime, timedelta
from time import time, sleep
import os
import sys
import sqlalchemy as sql
import sqlalchemy.exc as sql_exec
import pandas as pd
from sqlalchemy import insert, select, text

from common import logger, today, data_dir
from db_config import engine_str, use_sqlite, s_tbl_snap, n_tbl_snap, s_tbl_opt_greeks, n_tbl_opt_greeks, \
    s_tbl_opt_straddle, \
    n_tbl_opt_straddle, n_tbl_master, s_tbl_master

execute_retry = True
pool = sql.create_engine(engine_str, pool_size=10, max_overflow=5, pool_recycle=67, pool_timeout=30, echo=None)

threshold_limit = 10000


def insert_data(table: sql.Table, dict_data, engine_address=None, multi=False, ignore=False, truncate=False, retry=1,
                wait_period=5):
    """
    This is used to insert the data in dict format into the table
    :param table: SQLAlchemy Table Object
    :param dict_data: Data to be inserted
    :param engine_address: Define a custom engine string. Use default if None provided.
    :param multi: Whether to use multi query or not
    :param ignore: Use Insert Ignore while insertion
    :param truncate: Truncate table before insertion
    :param retry: Insert Retry Number
    :param wait_period: Time in seconds for retry
    :return: None
    """
    st = time()
    logger.debug(f'Data Insertion started for {table.name}')
    # # logger.info(f'Data Insertion started for {table.name}')
    # engine_con_str = engine_address if engine_address is not None else engine_str
    # engine = sql.create_engine(engine_con_str)
    ins = table.insert()
    if ignore:
        if use_sqlite:
            ignore_clause = 'IGNORE' if not use_sqlite else 'OR IGNORE'
            ins = table.insert().prefix_with(ignore_clause)
        else:
            # Considering Postgres
            from sqlalchemy.dialects.postgresql import insert
            # ins = table.insert().on_conflict_do_nothing()  # Not available
            ins = insert(table).on_conflict_do_nothing()

    with pool.connect() as conn:
        if truncate:
            conn.execute(f'TRUNCATE TABLE {table.name}')
        try:
            conn.execute(ins, dict_data, multi=multi)
        except sql_exec.OperationalError as e:
            if retry > 0:
                # # logger.info(f"Error for {table.name}: {e}")
                # # logger.info(f'Retrying to insert data in {table.name} after {wait_period} seconds')
                sleep(wait_period)
                insert_data(table=table, dict_data=dict_data, engine_address=engine_address, multi=multi, ignore=ignore,
                            truncate=truncate, retry=retry - 1, wait_period=wait_period)
            else:
                logger.error(f"Error for {table.name} insertion: {e}", escalate=True)
        conn.close()

    logger.debug(f"Data Inserted in {table.name} in: {time() - st} secs")
    # # logger.info(f"Data Inserted in {table.name} in: {time() - st} secs")


def insert_data_df(table, data: pd.DataFrame, truncate=False, master=False):
    conn = pool.connect()
    if truncate:
        conn.execute(f'TRUNCATE TABLE {table.name}')
    if master:
        st = time()
        # # logger.info(f'Data insertion started for master')
        response = data.to_sql(table, con=conn, if_exists='replace', index=False)
        # df.to_sql(tablename, conn, if_exists='replace', index=False)
        # # logger.info(f'Data insertion completed for master in: {time() - st} secs')
        return response
    else:
        response = data.to_sql(table.name, con=conn, if_exists='append', index=False, method='multi')
    conn.close()
    return response


def execute_query(query, retry=2, wait_period=5, params=None):
    if params is None:
        params = {}
    st = time()
    short_query = query[:int(len(query) * 0.25)] if type(query) is str else ''
    # logger.debug(f'Executing query...{short_query}...')
    # engine = sql.create_engine(engine_str)
    try:
        with pool.connect() as conn:
            try:
                result = conn.execute(query, params)
            except:
                result = conn.execute(sql.text(query), params).mappings()  # for login
            # conn.execute(ins, dict_data, multi=multi)
            conn.close()
    except sql_exec.OperationalError as e:
        if retry > 0:
            # # logger.info(f"Error for Query {short_query}: {e}")
            # # logger.info(f'Retrying to execute query {short_query} after {wait_period} seconds')
            sleep(wait_period)
            result = execute_query(query=query, retry=retry - 1, wait_period=wait_period)
        else:
            logger.error(f"Error for Query {short_query}: {e}", escalate=True)

    # logger.debug(f"Time taken to execute query: {time() - st} secs")
    return result


def read_sql_df(query, params=None, commit=False):
    st = time()
    # logger.debug(f"Reading query..{query[:int(len(query)*0.25)]}...")
    # # # logger.info(f"Reading query..{query[:int(len(query) * 0.25)]}...")
    # engine = sql.create_engine(engine_str)
    conn = pool.connect()
    df = pd.read_sql(query, conn, params=params)
    if commit:
        conn.execute('commit')
    conn.close()
    # engine.dispose()
    # logger.debug(f'Data read in {time() - st} secs')
    # # logger.info(f'Data read in {time() - st} secs')
    return df


def calculate_table_data(df):
    df1 = df.copy()
    live = (df1['combined_premium'].iloc[-1]).round(2)
    max_straddle = (df1['combined_premium'].max()).round(2)
    min_straddle = (df1['combined_premium'].min()).round(2)
    live_min = (live - min_straddle).round(2)
    max_live = (max_straddle - live).round(2)
    ret_dict = [{
        'Live': live,
        'Live-Min': live_min,
        'Max-Live': max_live,
        'Max': max_straddle,
        'Min': min_straddle
    }]
    # # # logger.info(f'\nret_dict is {ret_dict}')
    # ret_df = pd.DataFrame.from_dict(ret_dict)
    # dict_to_json = [{i:ret_dict[i]} for i in ret_dict]
    # # # logger.info(f'\n dict_to_json is {dict_to_json}')
    return ret_dict


class DBHandler:
    """
    Meant to handle Signal related stuff only.
    """

    @classmethod
    def check_user_exist(cls, email):
        # query = '''SELECT * FROM users;'''
        # response = execute_query(query)

        query = '''SELECT email, password FROM chart_users WHERE email = :email'''
        response = execute_query(query, params={"email": email})
        data = response.fetchone()

        if data is None:
            return False, False
        else:
            return True, data

    @classmethod
    def build_users_params(cls, users: list):
        users_in = {f"users_{_i}": _u for _i, _u in enumerate(users)}
        params_in = ",".join([f"%({i})s" for i in users_in.keys()])
        return users_in, params_in

    @classmethod
    def insert_snap_data(cls, db_data: list[dict]):
        # # logger.info('insert_snap>insert_data')
        insert_data(s_tbl_snap, db_data, ignore=True)

    @classmethod
    def get_snap_data(cls, ts: datetime):
        query = f"""
            SELECT * FROM {n_tbl_snap} WHERE "timestamp"='{ts}'
        """
        result = execute_query(query, params={'ts': ts})
        response = result.fetchall()
        return response

    @classmethod
    def insert_opt_greeks(cls, db_data: list[dict]):
        insert_data(s_tbl_opt_greeks, db_data, ignore=True)

    @classmethod
    def insert_opt_straddle(cls, db_data: list[dict]):
        insert_data(s_tbl_opt_straddle, db_data, ignore=True)

    @classmethod
    def get_straddle_minima(cls, symbol, expiry, start_from=today, table: bool = False):
        start_from1 = start_from.replace(hour=9, minute=16, second=0)
        query = f"""
            SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv
            FROM {n_tbl_opt_straddle}
            WHERE underlying='{symbol}' and date(expiry)='{expiry}' and minima='true' and "timestamp"::timestamp>='{start_from1}';
        """
        df = read_sql_df(query)
        # df = execute_query(query)

        if table:
            # # logger.info(f'\ndf made from read_sql_df is \n{df.head()}')
            # # logger.info(f"\nLive is {df['combined_premium'].iloc[-1]} max = {df['combined_premium'].max()} \t min is {df['combined_premium'].min()}")
            # # logger.info('\n df sent for trucation')
            table_df = calculate_table_data(df)
            return table_df
        else:
            return df
        # return df

    @classmethod
    def get_straddle_minima_table(cls, symbol, expiry, start_from=today.replace(hour=9, minute=15)):
        query = f"""
            SELECT timestamp as ts, combined_premium
            FROM {n_tbl_opt_straddle}
            WHERE underlying='{symbol}' and date(expiry)='{expiry}' and minima='true' and "timestamp">'{start_from}';
        """
        df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
        # # # logger.info(f'\ndf made from read_sql_df is \n{df.head()}')
        # # logger.info(f"\nLive is {df['combined_premium'].iloc[-1]} max = {df['combined_premium'].max()} \t min is {df['combined_premium'].min()}")
        # # # logger.info('\n df sent for trucation')
        table_dict = calculate_table_data(df)
        return table_dict

    # @classmethod
    # def get_straddle_iv_data(cls, symbol, expiry, start_from=today):
    #     query = f"""
    #             SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv, minima
    #             FROM {n_tbl_opt_straddle}
    #             WHERE underlying=%(symbol)s and expiry=%(expiry)s and "timestamp">='{start_from}';
    #         """
    #     df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
    #     return df

    @classmethod
    def get_straddle_iv_data(cls, symbol, expiry, start_from=today):
        query = f"""
                    SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv, minima
                    FROM {n_tbl_opt_straddle}
                    WHERE underlying='{symbol}' and date(expiry)='{expiry}' and "timestamp"::timestamp>='{start_from}' and call_oi > {threshold_limit} and put_oi > {threshold_limit} and combined_premium is not null;
                """
        df = read_sql_df(query)
        # # # logger.info(f'strd iv df for {symbol} {expiry} is\n  {df}')
        # df = df[(df['call_oi']>10000) & (df['put_oi']>10000)]
        # tr = datetime.now().time().replace(microsecond=0).strftime("%H_%M_%S")
        # df.to_csv(os.path.join(data_dir, f'iv_data_{symbol}_{expiry}_{tr}.csv'))
        return df

    # @classmethod
    # def get_credentials(cls):
    #     query = f"""
    #             SELECT * FROM {s_tbl_creds} WHERE status = 'active'
    #         """
    #     df = read_sql_df(query)
    #     return df

    # @classmethod
    # def insert_credentials(cls, db_data):
    #     insert_data(s_tbl_creds, db_data, ignore=True)

    # @classmethod
    # def update_credentials(cls, appkey, new_token):
    #     update_query = f"""
    #                     UPDATE creds SET token = '{new_token}' WHERE appkey = '{appkey}'
    #                 """
    #     result = execute_query(update_query)
    #     # # logger.info(result)

    @classmethod
    def delete_old_data(cls):
        table_list = [n_tbl_snap, n_tbl_opt_greeks, n_tbl_opt_straddle]
        a = (today - timedelta(days=30)).date().strftime("%Y-%m-%d")
        # b = pd.to_datetime('2024-05-20').date().strftime('%Y-%m-%d')
        # c = '2024-05-20'
        for each_table in table_list:
            delete_query = f"""
                        DELETE FROM {each_table} WHERE "timestamp"<:cutoff_date
                    """
            result = execute_query(delete_query, params={'cutoff_date': a})
            # # logger.info(result)
        return True

# res = DBHandler.get_straddle_iv_data(symbol = 'NIFTY', expiry = '2024-07-25')
# # # logger.info(res)