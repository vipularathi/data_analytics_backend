# from datetime import date, datetime, time
#
# import numpy as np
# import pandas as pd
# import uvicorn
# from fastapi import FastAPI, Query, status
# from fastapi.encoders import jsonable_encoder
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
#
# from common import IST, yesterday, today, logger, fixed_response_dict
# from contracts import get_req_contracts
# from db_ops import DBHandler
# from pydantic import BaseModel
# from passlib.context import CryptContext
#
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
#
#
# class UserL(BaseModel):
#     username: str
#     password: str
#
#
# class ServiceApp:
#
#     def __init__(self):
#         super().__init__()
#         self.app = FastAPI(title='ARathi', description='ARathi', docs_url='/docs', openapi_url='/openapi.json')
#         self.app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"],
#                                 allow_headers=["*"])
#
#         self.add_routes()
#         self.symbol_expiry_map = None
#         self.use_otm_iv = True
#         self.copy_symbol_expiry_map = None
#
#     def add_routes(self):
#         self.app.add_api_route('/', methods=['GET'], endpoint=self.default)
#         self.app.add_api_route('/symbol', methods=['GET'], endpoint=self.get_symbols)
#         self.app.add_api_route('/straddle/minima', methods=['GET'], endpoint=self.fetch_straddle_minima)
#         self.app.add_api_route('/straddle/minima/table', methods=['GET'],
#                                endpoint=self.fetch_straddle_minima_table)  # NEW
#         self.app.add_api_route('/straddle/iv', methods=['GET'], endpoint=self.fetch_straddle_iv)
#         self.app.add_api_route('/straddle/cluster', methods=['GET'], endpoint=self.fetch_straddle_cluster)
#         self.app.add_api_route('/login', methods=['POST'], endpoint=self.userLogin)
#
#     @staticmethod
#     def default():
#         body = {'status': 'success', 'message': '', 'data': [], 'response_code': None}
#         return JSONResponse(content=jsonable_encoder(body), status_code=status.HTTP_200_OK)
#
#     def isUserExist(self, username, password):
#         msg, data = DBHandler.check_user_exist(username)
#         if msg:
#             # if not data.get("active", False):
#             #     return False, "user is inactive"
#
#             if data['email'] == 'test@rathi.com' and data['password'] in ['test']:
#                 return True, data
#
#             # hashed_password = data.get("pwd", '')
#             # if not pwd_context.verify(user.password, hashed_password):
#             #     return False, 'Incorrect Password'
#
#             return True, data
#         else:
#             return False, "User not exist"
#
#     def userLogin(self, username, password) -> dict:
#         '''
#         /login - POST - user sends username and password and it is verified and
#          if successful then send role and token in the response.
#          (Token generated in this step to be stored in the token table)
#         '''
#         # check the user exists or not in user table
#         msg, res = self.isUserExist(username, password)
#
#         if msg and res['email'] == username and res['password'] == password:
#             return {"msg": True, "output": "login success"}
#         else:
#             return {"msg": True, "output": "login failure"}
#
#         # if not msg:
#         #     return self.success_response(message=res, addn_body={"username": username, "password": password})
#
#         # output = {"role": res.get("role", "user"), "token": ""}
#
#         # getting the token from token table
#         # res = getToken(res['id'])
#         # if not res:
#         #     return self.success_response(message="unable to generate token for user",
#         #                                  addn_body={"username": username, "password": password})
#
#         # output['token'] = res[1]
#         # return self.success_response(message="login success", addn_body=output)
#
#     def get_symbols(self):
#         if self.symbol_expiry_map is None:
#             ins_df, tokens, token_xref = get_req_contracts()
#             ins_df['expiry'] = ins_df['expiry'].dt.strftime('%Y-%m-%d')
#             agg = ins_df[ins_df['instrument_type'].isin(['CE', 'PE'])].groupby(['name'], as_index=False).agg(
#                 {'expiry': set, 'tradingsymbol': 'count'})
#             agg['expiry'] = agg['expiry'].apply(lambda x: sorted(list(x)))
#             self.symbol_expiry_map = agg.to_dict('records')
#             self.copy_symbol_expiry_map = self.symbol_expiry_map.copy()
#         return self.symbol_expiry_map
#
#     # def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
#     #                           interval: int = Query(1), cont: bool = Query(False)):
#     #     logger.info(f'inside fetch_straddle_minima and cont is {cont}')
#     #     if cont:
#     #         df = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
#     #         df['prev'] = df['ts'] < today
#     #         logger.info(f'fetch straddle with 1 {symbol} {expiry}')
#     #     else:
#     #         df = DBHandler.get_straddle_minima(symbol, expiry)
#     #         df['prev'] = False
#     #         logger.info(f'fetch straddle with 2 {symbol} {expiry}')
#     #     if self.use_otm_iv:
#     #         df['combined_iv'] = df['otm_iv']
#     #     logger.info(f'\nstraddle_minima df is \n {df}')
#     #     return self._straddle_response(df, count=st_cnt, interval=interval)
#     #
#     #
#     # # def fetch_straddle_minima_table(self, st_cnt: int = Query(default=None), interval: int = Query(1),
#     # #                                 cont: bool = Query(False), table: bool = Query(True)):
#     # #     if self.copy_symbol_expiry_map:
#     # #         logger.info(f'\nsym exp map is {self.copy_symbol_expiry_map}')
#     # #         for_table = []
#     # #         for i in range(len(self.copy_symbol_expiry_map)):
#     # #             #     print(f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
#     # #             logger.info(
#     # #                 f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
#     # #             name = self.copy_symbol_expiry_map[i]['name'];
#     # #             sorted_exp = sorted(self.copy_symbol_expiry_map[i]['expiry'])
#     # #             if name == 'NIFTY':
#     # #                 new_exp = sorted_exp[:2]
#     # #                 dict_1 = {'NIFTY_CW': new_exp[0], 'NIFTY_NW': new_exp[1]}
#     # #                 for_table.append(dict_1)
#     # #             elif name == 'BANKNIFTY':
#     # #                 new_exp = sorted_exp[:2]
#     # #                 dict_1 = {'BANKNIFTY_CW': new_exp[0], 'BANKNIFTY_NW': new_exp[1]}
#     # #                 for_table.append(dict_1)
#     # #             elif name == 'FINNIFTY':
#     # #                 dict_1 = {'FINNIFTY': sorted_exp[0]}
#     # #                 for_table.append(dict_1)
#     # #             else:
#     # #                 new_exp = sorted_exp[0]
#     # #                 dict_1 = {'MIDCPNIFTY': new_exp}
#     # #                 for_table.append(dict_1)
#     # #         # print('\n for table dict is ', for_table)
#     # #         logger.info(f'\nfor_table dict is {for_table}')
#     # #
#     # #         final_json = []
#     # #         for i in for_table:
#     # #             for symbol, expiry in i.items():
#     # #                 # print(f'\n key{count} is {key} and value{count} is {value}')
#     # #                 logger.info((f'\n original key is {symbol} and value is {expiry}'))
#     # #                 if symbol.startswith('NIFTY'):
#     # #                     symbol1 = 'NIFTY'
#     # #                 elif symbol.startswith('BANK'):
#     # #                     symbol1 = 'BANKNIFTY'
#     # #                 elif symbol.startswith('FIN'):
#     # #                     symbol1 = 'FINNIFTY'
#     # #                 else:
#     # #                     symbol1 = 'MIDCPNIFTY'
#     # #                 logger.info(f'\n changed key is {symbol1} and value is {expiry}')
#     # #                 list_dict_resp = DBHandler.get_straddle_minima_table(symbol1, expiry)
#     # #                 logger.info(f'\nlist_dict_resp is {list_dict_resp}')
#     # #                 # if symbol.startswith('NIFTY_CW'):
#     # #                 #     symbol2 = 'NF CW'
#     # #                 # elif symbol.startswith('BANKNIFTY_CW'):
#     # #                 #     symbol2 = 'BN CW'
#     # #                 # elif symbol.startswith('FINNIFTY'):
#     # #                 #     symbol2 = 'FN CW'
#     # #                 # elif symbol.startswith('MIDCPNIFTY'):
#     # #                 #     symbol2 = 'MN CW'
#     # #                 # elif symbol.startswith('NIFTY_NW'):
#     # #                 #     symbol2 = 'NF NW'
#     # #                 # elif symbol.startswith('BANKNIFTY_NW'):
#     # #                 #     symbol2 = 'BN NW'
#     # #                 new_dict = {symbol: list_dict_resp}
#     # #                 logger.info(f'\nnew_dict is {new_dict}')
#     # #                 final_json.append(new_dict)
#     # #                 logger.info(f'\nmaking final json resp- {final_json}')
#     # #
#     # #         logger.info(f'\nFINAL JSON RESP IS {final_json}')
#     # #         # df_json = df.to_json()
#     # #         # logger.info(f'\n df to json is {df_json}')
#     # #         # df_json = df.to_dict('records')
#     # #         # self.copy_symbol_expiry_map = None
#     # #         return final_json
#     #
#     # def fetch_straddle_minima_table(self, st_cnt: int = Query(default=None), interval: int = Query(1),
#     #                                 cont: bool = Query(False), table: bool = Query(True)):
#     #     if self.copy_symbol_expiry_map:
#     #         # logger.info(f'\nsym exp map is {self.copy_symbol_expiry_map}')
#     #         for_table = []
#     #         current_time = datetime.now().time()
#     #         if current_time > time(9,15):
#     #             for i in range(len(self.copy_symbol_expiry_map)):
#     #                 #     print(f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
#     #                 # logger.info(
#     #                 #     f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
#     #                 name = self.copy_symbol_expiry_map[i]['name']
#     #                 sorted_exp = sorted(self.copy_symbol_expiry_map[i]['expiry'])
#     #                 if name == 'NIFTY':
#     #                     new_exp = sorted_exp[:2]
#     #                     dict_1 = {'NIFTY_CW': new_exp[0], 'NIFTY_NW': new_exp[1]}
#     #                     for_table.append(dict_1)
#     #                 elif name == 'BANKNIFTY':
#     #                     new_exp = sorted_exp[:2]
#     #                     dict_1 = {'BANKNIFTY_CW': new_exp[0], 'BANKNIFTY_NW': new_exp[1]}
#     #                     for_table.append(dict_1)
#     #                 elif name == 'FINNIFTY':
#     #                     dict_1 = {'FINNIFTY': sorted_exp[0]}
#     #                     for_table.append(dict_1)
#     #                 else:
#     #                     new_exp = sorted_exp[0]
#     #                     dict_1 = {'MIDCPNIFTY': new_exp}
#     #                     for_table.append(dict_1)
#     #             # print('\n for table dict is ', for_table)
#     #             # logger.info(f'\nfor_table dict is {for_table}')
#     #
#     #             final_json = []
#     #             for i in for_table:
#     #                 for symbol, expiry in i.items():
#     #                     # print(f'\n key{count} is {key} and value{count} is {value}')
#     #                     # logger.info((f'\n original key is {symbol} and value is {expiry}'))
#     #                     if symbol.startswith('NIFTY'):
#     #                         symbol1 = 'NIFTY'
#     #                     elif symbol.startswith('BANK'):
#     #                         symbol1 = 'BANKNIFTY'
#     #                     elif symbol.startswith('FIN'):
#     #                         symbol1 = 'FINNIFTY'
#     #                     else:
#     #                         symbol1 = 'MIDCPNIFTY'
#     #                     # logger.info(f'\n changed key is {symbol1} and value is {expiry}')
#     #                     list_dict_resp = DBHandler.get_straddle_minima_table(symbol1, expiry)
#     #                     # logger.info(f'\nlist_dict_resp is {list_dict_resp}')
#     #                     # if symbol.startswith('NIFTY_CW'):
#     #                     #     symbol2 = 'NF CW'
#     #                     # elif symbol.startswith('BANKNIFTY_CW'):
#     #                     #     symbol2 = 'BN CW'
#     #                     # elif symbol.startswith('FINNIFTY'):
#     #                     #     symbol2 = 'FN CW'
#     #                     # elif symbol.startswith('MIDCPNIFTY'):
#     #                     #     symbol2 = 'MN CW'
#     #                     # elif symbol.startswith('NIFTY_NW'):
#     #                     #     symbol2 = 'NF NW'
#     #                     # elif symbol.startswith('BANKNIFTY_NW'):
#     #                     #     symbol2 = 'BN NW'
#     #                     new_dict = {symbol: list_dict_resp}
#     #                     # logger.info(f'\nnew_dict is {new_dict}')
#     #                     final_json.append(new_dict)
#     #                     # logger.info(f'\nmaking final json resp- {final_json}')
#     #
#     #             # logger.info(f'\nFINAL JSON RESP IS {final_json}')
#     #             # df_json = df.to_json()
#     #             # logger.info(f'\n df to json is {df_json}')
#     #             # df_json = df.to_dict('records')
#     #             # self.copy_symbol_expiry_map = None
#     #             return final_json
#     #         else:
#     #             # return None
#     #             empty_json = [
#     #                     {
#     #                         "BANKNIFTY_CW": [
#     #                             {
#     #                                 "Live": 0,
#     #                                 "Live-Min": 0,
#     #                                 "Max-Live": 0,
#     #                                 "Max": 0,
#     #                                 "Min": 0
#     #                             }
#     #                         ]
#     #                     },
#     #                     {
#     #                         "BANKNIFTY_NW": [
#     #                             {
#     #                                 "Live": 0,
#     #                                 "Live-Min": 0,
#     #                                 "Max-Live": 0,
#     #                                 "Max": 0,
#     #                                 "Min": 0
#     #                             }
#     #                         ]
#     #                     },
#     #                     {
#     #                         "FINNIFTY": [
#     #                             {
#     #                                 "Live": 0,
#     #                                 "Live-Min": 0,
#     #                                 "Max-Live": 0,
#     #                                 "Max": 0,
#     #                                 "Min": 0
#     #                             }
#     #                         ]
#     #                     },
#     #                     {
#     #                         "MIDCPNIFTY": [
#     #                             {
#     #                                 "Live": 0,
#     #                                 "Live-Min": 0,
#     #                                 "Max-Live": 0,
#     #                                 "Max": 0,
#     #                                 "Min": 0
#     #                             }
#     #                         ]
#     #                     },
#     #                     {
#     #                         "NIFTY_CW": [
#     #                             {
#     #                                 "Live": 0,
#     #                                 "Live-Min": 0,
#     #                                 "Max-Live": 0,
#     #                                 "Max": 0,
#     #                                 "Min": 0
#     #                             }
#     #                         ]
#     #                     },
#     #                     {
#     #                         "NIFTY_NW": [
#     #                             {
#     #                                 "Live": 0,
#     #                                 "Live-Min": 0,
#     #                                 "Max-Live": 0,
#     #                                 "Max": 0,
#     #                                 "Min": 0
#     #                             }
#     #                         ]
#     #                     }
#     #                 ]
#     #             return empty_json
#     #
#     #
#     # def fetch_straddle_iv(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
#     #                       interval: int = Query(5)):
#     #     df = DBHandler.get_straddle_iv_data(symbol, expiry)
#     #     if self.use_otm_iv:
#     #         df['combined_iv'] = df['otm_iv']
#     #     return self._straddle_response(df, count=st_cnt, interval=interval)
#     #
#     # # def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=15),
#     # #                            interval: int = Query(5)):
#     # #     all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday)
#     # #     all_data = []
#     # #     today_df = all_df[all_df['ts'] >= today].copy()
#     # #     prev_df = all_df[all_df['ts'] < today].copy()
#     # #     if len(prev_df):
#     # #         max_ts = prev_df['ts'].max()
#     # #         prev_df = prev_df[prev_df['ts'] == max_ts].copy()
#     # #         all_data.append(prev_df)
#     # #     if len(today_df):
#     # #         all_data.append(today_df)
#     # #
#     # #     if all_data:
#     # #         df = pd.concat(all_data, ignore_index=True, sort=False)
#     # #     else:
#     # #         df = all_df.iloc[:0]
#     # #     if self.use_otm_iv:
#     # #         df['combined_iv'] = df['otm_iv']
#     # #     # allowed = pd.date_range(df['ts'].min(), df['ts'].max(), freq=interval)
#     # #     # req = df[df['ts'].isin(allowed)].copy()
#     # #     break_ts = time(12, 30, 0)
#     # #     req1 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#     # #     req1 = req1[req1['ts'].dt.time <= break_ts].copy()
#     # #     req2 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#     # #     req2 = req2[req2['ts'].dt.time > break_ts].copy()  # prev day covered here
#     # #     d = [req1, req2]
#     # #     # d = [req1]
#     # #     d = [_d for _d in d if len(d)]
#     # #     if d:
#     # #         req = pd.concat(d, ignore_index=True, sort=False)
#     # #         req.sort_values(['ts', 'strike'], inplace=True)
#     # #     else:
#     # #         req = pd.DataFrame(columns=req1.columns)
#     # #     req = req.replace({np.NAN: None}).round(2)
#     # #     strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
#     # #     strike_iv.sort_values(['strike'], inplace=True)
#     # #     strikes = strike_iv['strike'].tolist()
#     # #     iv = list(zip(*strike_iv['combined_iv'].tolist()))
#     # #     ts = list(zip(*strike_iv['ts'].tolist()))
#     # #     return {'strikes': strikes, 'iv': iv, 'ts': ts}
#     #
#     # def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=15),
#     #                            interval: int = Query(5)):
#     #     all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday)
#     #     all_data = []
#     #     today_df = all_df[all_df['ts'] >= today].copy()
#     #     prev_df = all_df[all_df['ts'] < today].copy()
#     #     if len(prev_df):
#     #         max_ts = prev_df['ts'].max()
#     #         prev_df = prev_df[prev_df['ts'] == max_ts].copy()
#     #         all_data.append(prev_df)
#     #     if len(today_df):
#     #         all_data.append(today_df)
#     #
#     #     if all_data:
#     #         df = pd.concat(all_data, ignore_index=True, sort=False)
#     #     else:
#     #         df = all_df.iloc[:0]
#     #     if self.use_otm_iv:
#     #         df['combined_iv'] = df['otm_iv']
#     #     # allowed = pd.date_range(df['ts'].min(), df['ts'].max(), freq=interval)
#     #     # req = df[df['ts'].isin(allowed)].copy()
#     #     break_ts = time(12, 30, 0)
#     #     # req1 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#     #     # req1 = req1[req1['ts'].dt.time <= break_ts].copy()
#     #     # req2 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#     #     # req2 = req2[req2['ts'].dt.time > break_ts].copy()  # prev day covered here
#     #     # d = [req1, req2]
#     #     # d = [_d for _d in d if len(d)]
#     #     # if d:
#     #     #     req = pd.concat(d, ignore_index=True, sort=False)
#     #     #     req.sort_values(['ts', 'strike'], inplace=True)
#     #     req = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#     #     if req is not None:
#     #         req.sort_values(['ts', 'strike'], inplace=True)
#     #     else:
#     #         # req = pd.DataFrame(columns=req1.columns)
#     #         req = pd.DataFrame(columns=req.columns)
#     #     req = req.replace({np.NAN: None}).round(2)
#     #     strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
#     #     strike_iv.sort_values(['strike'], inplace=True)
#     #     strikes = strike_iv['strike'].tolist()
#     #     iv = list(zip(*strike_iv['combined_iv'].tolist()))
#     #     ts = list(zip(*strike_iv['ts'].tolist()))
#     #     return {'strikes': strikes, 'iv': iv, 'ts': ts}
#     #
#     # def _straddle_response(self, df: pd.DataFrame, raw=False, count: int = None, interval: int = None):
#     #     count = 10 if count is None else count
#     #     l_st, u_st = count + 1, count
#     #     # df['range'] = (df['spot'] - df['strike']).abs() < (df['spot'] * 0.05)
#     #     # strikes = df[df['range']]['strike'].unique()
#     #     mean = df['spot'].mean()
#     #     uq_strikes = df['strike'].unique()
#     #     uq_strikes.sort()
#     #     strikes = uq_strikes[uq_strikes <= mean][-l_st:].tolist() + uq_strikes[uq_strikes > mean][:u_st].tolist()
#     #     # print(uq_strikes, strikes)
#     #     df: pd.DataFrame = df[df['strike'].isin(strikes)].copy()
#     #     df.drop(columns=['spot', 'range'], errors='ignore', inplace=True)
#     #     df.sort_values(['ts', 'strike'], inplace=True)
#     #     if interval and len(df):
#     #         valid_ts = pd.date_range(start=df['ts'].min(), end=df['ts'].max(), freq=f'{interval}min')
#     #         if len(valid_ts):
#     #             df = df[df['ts'].isin(valid_ts)].copy()
#     #     if raw:
#     #         return df
#     #     logger.info(f'straddle response df is \n {df.head()}')
#     #     return self.df_response(df, to_millis=['ts'])
#     #
#     # @staticmethod
#     # def df_response(df: pd.DataFrame, to_millis: list = None) -> list[dict]:
#     #     df = df.replace({np.NAN: None}).round(2)
#     #     dict1 = df.to_dict('records')
#     #     logger.info(f'\ndf_response_dict1[0] before epoch conversion is {dict1[0]}')
#     #
#     #     # converting local time to unix time
#     #     if to_millis is not None and len(to_millis) and len(df):
#     #         for _col in to_millis:
#     #             df[_col] = (df[_col].dt.tz_localize(IST).astype('int64') // 10 ** 9) * 1000
#     #
#     #     dict2 = df.to_dict('records')
#     #     logger.info(f'\ndf_response_dict1[0] after epoch conversion is {dict2[0]}')
#     #     # for _key, _value in dict1[0].items():
#     #     #     print(f'\n1st line of dftodict is {_key}:{_value}')
#     #     # count = 0
#     #     # for _entity in dict1[0]:
#     #     #     for _key, _value in _entity.items():
#     #     #         logger.info(f'\n1st line of dftodict is {_key}:{_value}')
#     #     #         if count == 0:
#     #     #             break
#     #     # logger.info(f'dict1 is {dict1}')
#     #     return df.to_dict('records')
#     # # response is LIST OF DICTIONARIES
#     # # sample response = {"ts":1714384740000,"strike":22700.0,"combined_premium":190.1,"combined_iv":11.52,"otm_iv":11.52,"prev":false}
#
#     def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
#                               interval: int = Query(1), cont: bool = Query(False)):
#         # logger.info(f'{symbol} {expiry} and cont is {cont}')
#         if cont:
#             df_yest = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
#             df_yest['prev'] = df_yest['ts'] < today
#             df_yest = df_yest[df_yest['prev'] == True]
#             # # logger.info(f'fetch straddle with 1 {symbol} {expiry}')
#             df_today = DBHandler.get_straddle_minima(symbol, expiry, start_from=today)
#             df_today['prev'] = df_today['ts'] < today
#             # df_orig = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
#             # df_yest = df_orig[df_orig['ts'] < today].copy()
#             # df_yest['prev'] = True
#             # df_today = df_orig[df_orig['ts'] >= today].copy()
#             # df_today['prev'] = False
#         else:
#             df = DBHandler.get_straddle_minima(symbol, expiry)
#             df['prev'] = False
#             # # logger.info(f'fetch straddle with 2 {symbol} {expiry}')
#         if self.use_otm_iv:
#             df_yest['combined_iv'] = df_yest['otm_iv']
#             df_today['combined_iv'] = df_today['otm_iv']
#         # # logger.info(f'\ndf of {symbol} {expiry} fetched from db is \n {df}')
#
#         # if symbol == 'NIFTY' and expiry == '2024-07-18':
#         #     df = pd.read_csv(r"D:\iv_charts_2\iv_filter_2\data_analytics_backend\test_time_data_nifty_cw.csv", index = False)
#         #     fixed_df = pd.read_csv(r"D:\iv_charts_2\iv_filter_2\data_analytics_backend\test_time_data_nifty_cw.csv",
#         #                      index=False)
#
#         fixed_resp = fixed_response_dict()
#         fixed_df = pd.DataFrame(fixed_resp)
#         # # logger.info(f'\nfixed df is \n{fixed_df.head()}')
#         df_yest['ts'] = pd.to_datetime(df_yest['ts'])
#         df_today['ts'] = pd.to_datetime(df_today['ts'])
#         fixed_df['ts'] = pd.to_datetime(fixed_df['ts'])
#         # logger.info(f'changed ts in all 3 df')
#
#         # # mtd-1
#         # merged_df = pd.merge(fixed_df, df[['ts', 'strike', 'combined_premium']], on='ts', how='left')
#         # # logger.info(f'orig merged df is \n{merged_df}')
#         # merged_df['strike'] = merged_df['strike'].fillna(0).astype(int)
#         # # logger.info(f'merged df after strike is \n {merged_df}')
#         # merged_df['combined_premium'] = merged_df['combined_premium'].fillna(0).astype(int)
#         # # logger.info(f'merged df after combined_premium is \n {merged_df}')
#
#         # mtd-2
#         fixed_df.set_index('ts')
#         # logger.info(f'fixed df {symbol} {expiry} is {fixed_df}')
#         df_today.set_index('ts')
#         # logger.info(f'df today {symbol} {expiry} is {df_today}')
#         df_yest.set_index('ts')
#         # logger.info(f'df yest {symbol} {expiry} is {df_yest}')
#         fixed_df.update(df_today[['spot','strike', 'combined_premium','combined_iv', 'otm_iv', 'prev']])
#         merged_df = fixed_df.copy()
#         # logger.info(f'updated merged df {symbol} {expiry} is \n{merged_df}')
#         final_df = pd.concat([df_yest, merged_df], axis=0)
#         # final_df[[]].fillna(0, inplace=True)
#         # final_df = final_df.apply(lambda col: col.fillna(0) if col.name != 'prev' else col)
#         # df_yest.update(merged_df[['spot','strike', 'combined_premium','combined_iv', 'otm_iv']])
#         # final_df = df_yest.copy()
#         # logger.info(f'final df {symbol} {expiry} after updation is \n{final_df}')
#
#         # # logger.info(f'merged df {symbol} {expiry} after updation is \n{final_df}')
#         final_df.reset_index()
#
#         # logger.info(f'\nmerged_df {symbol} {expiry} is \n {final_df}')
#         return self._straddle_response(final_df, count=st_cnt, interval=interval)
#
#     def fetch_straddle_minima_table(self, st_cnt: int = Query(default=None), interval: int = Query(1),
#                                     cont: bool = Query(False), table: bool = Query(True)):
#         if self.copy_symbol_expiry_map:
#             # # logger.info(f'\nsym exp map is {self.copy_symbol_expiry_map}')
#             for_table = []
#             current_time = datetime.now().time()
#             if current_time > time(9,15):
#                 for i in range(len(self.copy_symbol_expiry_map)):
#                     #     print(f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
#                     # # logger.info(
#                     #     f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
#                     name = self.copy_symbol_expiry_map[i]['name']
#                     sorted_exp = sorted(self.copy_symbol_expiry_map[i]['expiry'])
#                     if name == 'NIFTY':
#                         new_exp = sorted_exp[:2]
#                         dict_1 = {'NIFTY_CW': new_exp[0], 'NIFTY_NW': new_exp[1]}
#                         for_table.append(dict_1)
#                     elif name == 'BANKNIFTY':
#                         new_exp = sorted_exp[:2]
#                         dict_1 = {'BANKNIFTY_CW': new_exp[0], 'BANKNIFTY_NW': new_exp[1]}
#                         for_table.append(dict_1)
#                     elif name == 'FINNIFTY':
#                         dict_1 = {'FINNIFTY': sorted_exp[0]}
#                         for_table.append(dict_1)
#                     else:
#                         new_exp = sorted_exp[0]
#                         dict_1 = {'MIDCPNIFTY': new_exp}
#                         for_table.append(dict_1)
#                 # print('\n for table dict is ', for_table)
#                 # # logger.info(f'\nfor_table dict is {for_table}')
#
#                 final_json = []
#                 for i in for_table:
#                     for symbol, expiry in i.items():
#                         # print(f'\n key{count} is {key} and value{count} is {value}')
#                         # # logger.info((f'\n original key is {symbol} and value is {expiry}'))
#                         if symbol.startswith('NIFTY'):
#                             symbol1 = 'NIFTY'
#                         elif symbol.startswith('BANK'):
#                             symbol1 = 'BANKNIFTY'
#                         elif symbol.startswith('FIN'):
#                             symbol1 = 'FINNIFTY'
#                         else:
#                             symbol1 = 'MIDCPNIFTY'
#                         # # logger.info(f'\n changed key is {symbol1} and value is {expiry}')
#                         list_dict_resp = DBHandler.get_straddle_minima_table(symbol1, expiry)
#                         # # logger.info(f'\nlist_dict_resp is {list_dict_resp}')
#                         # if symbol.startswith('NIFTY_CW'):
#                         #     symbol2 = 'NF CW'
#                         # elif symbol.startswith('BANKNIFTY_CW'):
#                         #     symbol2 = 'BN CW'
#                         # elif symbol.startswith('FINNIFTY'):
#                         #     symbol2 = 'FN CW'
#                         # elif symbol.startswith('MIDCPNIFTY'):
#                         #     symbol2 = 'MN CW'
#                         # elif symbol.startswith('NIFTY_NW'):
#                         #     symbol2 = 'NF NW'
#                         # elif symbol.startswith('BANKNIFTY_NW'):
#                         #     symbol2 = 'BN NW'
#                         new_dict = {symbol: list_dict_resp}
#                         # # logger.info(f'\nnew_dict is {new_dict}')
#                         final_json.append(new_dict)
#                         # # logger.info(f'\nmaking final json resp- {final_json}')
#
#                 # # logger.info(f'\nFINAL JSON RESP IS {final_json}')
#                 # df_json = df.to_json()
#                 # # logger.info(f'\n df to json is {df_json}')
#                 # df_json = df.to_dict('records')
#                 # self.copy_symbol_expiry_map = None
#                 return final_json
#             else:
#                 # return None
#                 empty_json = [
#                         {
#                             "BANKNIFTY_CW": [
#                                 {
#                                     "Live": 0,
#                                     "Live-Min": 0,
#                                     "Max-Live": 0,
#                                     "Max": 0,
#                                     "Min": 0
#                                 }
#                             ]
#                         },
#                         {
#                             "BANKNIFTY_NW": [
#                                 {
#                                     "Live": 0,
#                                     "Live-Min": 0,
#                                     "Max-Live": 0,
#                                     "Max": 0,
#                                     "Min": 0
#                                 }
#                             ]
#                         },
#                         {
#                             "FINNIFTY": [
#                                 {
#                                     "Live": 0,
#                                     "Live-Min": 0,
#                                     "Max-Live": 0,
#                                     "Max": 0,
#                                     "Min": 0
#                                 }
#                             ]
#                         },
#                         {
#                             "MIDCPNIFTY": [
#                                 {
#                                     "Live": 0,
#                                     "Live-Min": 0,
#                                     "Max-Live": 0,
#                                     "Max": 0,
#                                     "Min": 0
#                                 }
#                             ]
#                         },
#                         {
#                             "NIFTY_CW": [
#                                 {
#                                     "Live": 0,
#                                     "Live-Min": 0,
#                                     "Max-Live": 0,
#                                     "Max": 0,
#                                     "Min": 0
#                                 }
#                             ]
#                         },
#                         {
#                             "NIFTY_NW": [
#                                 {
#                                     "Live": 0,
#                                     "Live-Min": 0,
#                                     "Max-Live": 0,
#                                     "Max": 0,
#                                     "Min": 0
#                                 }
#                             ]
#                         }
#                     ]
#                 return empty_json
#
#     def fetch_straddle_iv(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
#                           interval: int = Query(5)):
#         df = DBHandler.get_straddle_iv_data(symbol, expiry)
#         if self.use_otm_iv:
#             df['combined_iv'] = df['otm_iv']
#         return self._straddle_response(df, count=st_cnt, interval=interval)
#
#     # def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=15),
#     #                            interval: int = Query(5)):
#     #     all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday)
#     #     all_data = []
#     #     today_df = all_df[all_df['ts'] >= today].copy()
#     #     prev_df = all_df[all_df['ts'] < today].copy()
#     #     if len(prev_df):
#     #         max_ts = prev_df['ts'].max()
#     #         prev_df = prev_df[prev_df['ts'] == max_ts].copy()
#     #         all_data.append(prev_df)
#     #     if len(today_df):
#     #         all_data.append(today_df)
#     #
#     #     if all_data:
#     #         df = pd.concat(all_data, ignore_index=True, sort=False)
#     #     else:
#     #         df = all_df.iloc[:0]
#     #     if self.use_otm_iv:
#     #         df['combined_iv'] = df['otm_iv']
#     #     # allowed = pd.date_range(df['ts'].min(), df['ts'].max(), freq=interval)
#     #     # req = df[df['ts'].isin(allowed)].copy()
#     #     break_ts = time(12, 30, 0)
#     #     req1 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#     #     req1 = req1[req1['ts'].dt.time <= break_ts].copy()
#     #     req2 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#     #     req2 = req2[req2['ts'].dt.time > break_ts].copy()  # prev day covered here
#     #     d = [req1, req2]
#     #     # d = [req1]
#     #     d = [_d for _d in d if len(d)]
#     #     if d:
#     #         req = pd.concat(d, ignore_index=True, sort=False)
#     #         req.sort_values(['ts', 'strike'], inplace=True)
#     #     else:
#     #         req = pd.DataFrame(columns=req1.columns)
#     #     req = req.replace({np.NAN: None}).round(2)
#     #     strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
#     #     strike_iv.sort_values(['strike'], inplace=True)
#     #     strikes = strike_iv['strike'].tolist()
#     #     iv = list(zip(*strike_iv['combined_iv'].tolist()))
#     #     ts = list(zip(*strike_iv['ts'].tolist()))
#     #     return {'strikes': strikes, 'iv': iv, 'ts': ts}
#
#     def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=15),
#                                interval: int = Query(5)):
#         all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday)
#         logger.info(f'\nall_df fetched from query for {symbol} {expiry} is \n {all_df}')
#         all_data = []
#         today_df = all_df[all_df['ts'] >= today].copy()
#         # # logger.info(f'\ntoday df for {symbol} {expiry} is \n {today_df}')
#         prev_df = all_df[all_df['ts'] < today].copy()
#         # # logger.info(f'\nprev df for {symbol} {expiry} is \n {prev_df}')
#         if len(prev_df):
#             max_ts = prev_df['ts'].max()
#             prev_df = prev_df[prev_df['ts'] == max_ts].copy()
#             all_data.append(prev_df)
#         if len(today_df):
#             all_data.append(today_df)
#
#         # # logger.info(f'\nall data before append for {symbol} {expiry} is \n {all_data}')
#         if all_data:
#             df = pd.concat(all_data, ignore_index=True, sort=False)
#         else:
#             df = all_df.iloc[:0]
#         # # logger.info(f'\nall data after append for {symbol} {expiry} is \n {all_data}')
#         # # logger.info(f'\ndf data after append for {symbol} {expiry} is \n {df}')
#         if self.use_otm_iv:
#             df['combined_iv'] = df['otm_iv']
#         # allowed = pd.date_range(df['ts'].min(), df['ts'].max(), freq=interval)
#         # req = df[df['ts'].isin(allowed)].copy()
#         break_ts = time(12, 30, 0)
#         # req1 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#         # req1 = req1[req1['ts'].dt.time <= break_ts].copy()
#         # req2 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#         # req2 = req2[req2['ts'].dt.time > break_ts].copy()  # prev day covered here
#         # d = [req1, req2]
#         # d = [_d for _d in d if len(d)]
#         # if d:
#         #     req = pd.concat(d, ignore_index=True, sort=False)
#         #     req.sort_values(['ts', 'strike'], inplace=True)
#         # # logger.info(f'\n df being sent to straddle response is \n{df}')
#         req = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
#         if req is not None:
#             req.sort_values(['ts', 'strike'], inplace=True)
#         else:
#             # req = pd.DataFrame(columns=req1.columns)
#             req = pd.DataFrame(columns=req.columns)
#         req = req.replace({np.NAN: None}).round(2)
#         strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
#         strike_iv.sort_values(['strike'], inplace=True)
#         strikes = strike_iv['strike'].tolist()
#         iv = list(zip(*strike_iv['combined_iv'].tolist()))
#         ts = list(zip(*strike_iv['ts'].tolist()))
#         logger.info(f'\nfetch straddle cluster Response for {symbol} {expiry}: \nstrikes={strikes}, \niv={iv}, \nts={ts}')
#         return {'strikes': strikes, 'iv': iv, 'ts': ts}
#
#     def _straddle_response(self, df: pd.DataFrame, raw=False, count: int = None, interval: int = None):
#         count = 10 if count is None else count
#         l_st, u_st = count + 1, count
#         logger.info(f'\ndf spot is \n {df["spot"]}')
#         df['spot'] = pd.to_numeric(df['spot'], errors='coerce')
#         df = df.dropna(subset=['spot'])
#         mean = df['spot'].mean()
#         # # logger.info(f'\nmean is {mean}')
#         uq_strikes = pd.to_numeric(df['strike'], errors='coerce').dropna().unique()
#         # uq_strikes = df['strike'].unique()
#         uq_strikes.sort()
#         strikes = uq_strikes[uq_strikes <= mean][-l_st:].tolist() + uq_strikes[uq_strikes > mean][:u_st].tolist()
#         logger.info(f'\n uq_strikes are \n{uq_strikes}, \n strikes are \n{strikes}')
#         df: pd.DataFrame = df[df['strike'].isin(strikes)].copy()
#         # # logger.info(f'df before drop is \n {df}')
#         df.drop(columns=['spot', 'range'], errors='ignore', inplace=True)
#         df.sort_values(['ts', 'strike'], inplace=True)
#         # # logger.info(f'df after drop is \n {df}')
#         if interval and len(df):
#             valid_ts = pd.date_range(start=df['ts'].min(), end=df['ts'].max(), freq=f'{interval}min')
#             logger.info(f'Total timestamps generated: {valid_ts}')
#             if len(valid_ts):
#                 df = df[df['ts'].isin(valid_ts)].copy()
#             logger.info(f'Valid timestamps generated: {valid_ts}')
#         # # logger.info(f'\ndf after valid ts is \n {df}')
#         if raw:
#             return df
#         # # logger.info(f'\nstraddle response df is \n {df.head()}')
#         return self.df_response(df, to_millis=['ts'])
#
#     @staticmethod
#     def df_response(df: pd.DataFrame, to_millis: list = None) -> list[dict]:
#         df = df.replace({np.NAN: None}).round(2)
#         dict1 = df.to_dict('records')
#         # # logger.info(f'\ndf_response_dict1[0] before epoch conversion is {dict1[0]}')
#
#         # converting local time to unix time
#         if to_millis is not None and len(to_millis) and len(df):
#             for _col in to_millis:
#                 df[_col] = (df[_col].dt.tz_localize(IST).astype('int64') // 10 ** 9) * 1000
#
#         dict2 = df.to_dict('records')
#         # # logger.info(f'\ndf_response_dict1[0] after epoch conversion is {dict2[0]}')
#         # for _key, _value in dict1[0].items():
#         #     print(f'\n1st line of dftodict is {_key}:{_value}')
#         # count = 0
#         # for _entity in dict1[0]:
#         #     for _key, _value in _entity.items():
#         #         # logger.info(f'\n1st line of dftodict is {_key}:{_value}')
#         #         if count == 0:
#         #             break
#         logger.info(f'\nstraddle response df is {dict2}')
#         return df.to_dict('records')
#
# service = ServiceApp()
# app = service.app
#
# if __name__ == '__main__':
#     uvicorn.run('app:app', host='0.0.0.0', port=8851, workers=2)


from datetime import date, datetime, time
from itertools import zip_longest

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from common import IST, yesterday, today, logger, fixed_response_dict
from contracts import get_req_contracts
from db_ops import DBHandler
from pydantic import BaseModel
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class UserL(BaseModel):
    username: str
    password: str


class ServiceApp:

    def __init__(self):
        super().__init__()
        self.app = FastAPI(title='ARathi', description='ARathi', docs_url='/docs', openapi_url='/openapi.json')
        self.app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"],
                                allow_headers=["*"])

        self.add_routes()
        self.symbol_expiry_map = None
        self.use_otm_iv = True
        self.copy_symbol_expiry_map = None

    def add_routes(self):
        self.app.add_api_route('/', methods=['GET'], endpoint=self.default)
        self.app.add_api_route('/symbol', methods=['GET'], endpoint=self.get_symbols)
        self.app.add_api_route('/straddle/minima', methods=['GET'], endpoint=self.fetch_straddle_minima)
        self.app.add_api_route('/straddle/minima/table', methods=['GET'],
                               endpoint=self.fetch_straddle_minima_table)  # NEW
        self.app.add_api_route('/straddle/iv', methods=['GET'], endpoint=self.fetch_straddle_iv)
        self.app.add_api_route('/straddle/cluster', methods=['GET'], endpoint=self.fetch_straddle_cluster)
        self.app.add_api_route('/login', methods=['POST'], endpoint=self.userLogin)

    @staticmethod
    def default():
        body = {'status': 'success', 'message': '', 'data': [], 'response_code': None}
        return JSONResponse(content=jsonable_encoder(body), status_code=status.HTTP_200_OK)

    def isUserExist(self, username, password):
        msg, data = DBHandler.check_user_exist(username)
        if msg:
            # if not data.get("active", False):
            #     return False, "user is inactive"

            if data['email'] == 'test@rathi.com' and data['password'] in ['test']:
                return True, data

            # hashed_password = data.get("pwd", '')
            # if not pwd_context.verify(user.password, hashed_password):
            #     return False, 'Incorrect Password'

            return True, data
        else:
            return False, "User not exist"

    def userLogin(self, username, password) -> dict:
        '''
        /login - POST - user sends username and password and it is verified and
         if successful then send role and token in the response.
         (Token generated in this step to be stored in the token table)
        '''
        # check the user exists or not in user table
        msg, res = self.isUserExist(username, password)

        if msg and res['email'] == username and res['password'] == password:
            return {"msg": True, "output": "login success"}
        else:
            return {"msg": True, "output": "login failure"}

        # if not msg:
        #     return self.success_response(message=res, addn_body={"username": username, "password": password})

        # output = {"role": res.get("role", "user"), "token": ""}

        # getting the token from token table
        # res = getToken(res['id'])
        # if not res:
        #     return self.success_response(message="unable to generate token for user",
        #                                  addn_body={"username": username, "password": password})

        # output['token'] = res[1]
        # return self.success_response(message="login success", addn_body=output)

    def get_symbols(self):
        if self.symbol_expiry_map is None:
            ins_df, tokens, token_xref = get_req_contracts()
            ins_df['expiry'] = ins_df['expiry'].dt.strftime('%Y-%m-%d')
            agg = ins_df[ins_df['instrument_type'].isin(['CE', 'PE'])].groupby(['name'], as_index=False).agg(
                {'expiry': set, 'tradingsymbol': 'count'})
            agg['expiry'] = agg['expiry'].apply(lambda x: sorted(list(x)))
            self.symbol_expiry_map = agg.to_dict('records')
            self.copy_symbol_expiry_map = self.symbol_expiry_map.copy()
        return self.symbol_expiry_map

    # def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
    #                           interval: int = Query(1), cont: bool = Query(False)):
    #     # logger.info(f'{symbol} {expiry} and cont is {cont}')
    #     if cont:
    #         df_yest = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
    #         df_yest['prev'] = df_yest['ts'] < today
    #         df_yest = df_yest[df_yest['prev'] == True]
    #         # # logger.info(f'fetch straddle with 1 {symbol} {expiry}')
    #         df_today = DBHandler.get_straddle_minima(symbol, expiry, start_from=today)
    #         df_today['prev'] = df_today['ts'] < today
    #         # df_orig = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
    #         # df_yest = df_orig[df_orig['ts'] < today].copy()
    #         # df_yest['prev'] = True
    #         # df_today = df_orig[df_orig['ts'] >= today].copy()
    #         # df_today['prev'] = False
    #
    #         # df_orig = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
    #         # df_orig['prev'] = df_orig['ts'] < today
    #         # df_yest = df_orig[df_orig['prev'] == True].copy()
    #         # df_today = df_orig[df_orig['prev'] == False].copy()
    #     else:
    #         # df = DBHandler.get_straddle_minima(symbol, expiry)
    #         # df['prev'] = False
    #         # # logger.info(f'fetch straddle with 2 {symbol} {expiry}')
    #         df_today = DBHandler.get_straddle_minima(symbol, expiry)
    #         df_today['prev'] = False
    #     if self.use_otm_iv:
    #         if cont:
    #             df_yest['combined_iv'] = df_yest['otm_iv']
    #             df_today['combined_iv'] = df_today['otm_iv']
    #         else:
    #             df_today['combined_iv'] = df_today['otm_iv']
    #     # # logger.info(f'\ndf of {symbol} {expiry} fetched from db is \n {df}')
    #
    #     # if symbol == 'NIFTY' and expiry == '2024-07-18':
    #     #     df = pd.read_csv(r"D:\iv_charts_2\iv_filter_2\data_analytics_backend\test_time_data_nifty_cw.csv", index = False)
    #     #     fixed_df = pd.read_csv(r"D:\iv_charts_2\iv_filter_2\data_analytics_backend\test_time_data_nifty_cw.csv",
    #     #                      index=False)
    #
    #     fixed_resp = fixed_response_dict()
    #     fixed_df = pd.DataFrame(fixed_resp)
    #     # # logger.info(f'\nfixed df is \n{fixed_df.head()}')
    #     df_yest['ts'] = pd.to_datetime(df_yest['ts'])
    #     df_today['ts'] = pd.to_datetime(df_today['ts'])
    #     fixed_df['ts'] = pd.to_datetime(fixed_df['ts'])
    #     # logger.info(f'changed ts in all 3 df')
    #
    #     # # mtd-1
    #     # merged_df = pd.merge(fixed_df, df[['ts', 'strike', 'combined_premium']], on='ts', how='left')
    #     # # logger.info(f'orig merged df is \n{merged_df}')
    #     # merged_df['strike'] = merged_df['strike'].fillna(0).astype(int)
    #     # # logger.info(f'merged df after strike is \n {merged_df}')
    #     # merged_df['combined_premium'] = merged_df['combined_premium'].fillna(0).astype(int)
    #     # # logger.info(f'merged df after combined_premium is \n {merged_df}')
    #
    #     # mtd-2
    #     fixed_df.set_index('ts')
    #     # logger.info(f'fixed df {symbol} {expiry} is {fixed_df}')
    #     df_today.set_index('ts')
    #     # logger.info(f'df today {symbol} {expiry} is {df_today}')
    #     df_yest.set_index('ts')
    #     # logger.info(f'df yest {symbol} {expiry} is {df_yest}')
    #     fixed_df.update(df_today[['spot','strike', 'combined_premium','combined_iv', 'otm_iv', 'prev']])
    #     merged_df = fixed_df.copy()
    #     # logger.info(f'updated merged df {symbol} {expiry} is \n{merged_df}')
    #     final_df = pd.concat([df_yest, merged_df], axis=0)
    #     # final_df[[]].fillna(0, inplace=True)
    #     # final_df = final_df.apply(lambda col: col.fillna(0) if col.name != 'prev' else col)
    #     # df_yest.update(merged_df[['spot','strike', 'combined_premium','combined_iv', 'otm_iv']])
    #     # final_df = df_yest.copy()
    #     # logger.info(f'final df {symbol} {expiry} after updation is \n{final_df}')
    #
    #     # # logger.info(f'merged df {symbol} {expiry} after updation is \n{final_df}')
    #     final_df.reset_index()
    #
    #     # logger.info(f'\nmerged_df {symbol} {expiry} is \n {final_df}')
    #     return self._straddle_response(final_df, count=st_cnt, interval=interval)

    def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
                              interval: int = Query(1), cont: bool = Query(False)):
        # logger.info(f'{symbol} {expiry} and cont is {cont}')
        if cont:
            df_orig = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
            logger.info(f'\n{symbol} {expiry} {cont} {today} {yesterday} df orig is \n {df_orig}')
            # if symbol == 'NIFTY' and expiry == pd.to_datetime("2024-07-18").date():
            #     df_yest.to_csv('test_data_nifty_july182024.csv', index=False)
            #     logger.info('csv file made successfully')
            # df_yest = pd.read_csv(r"D:\iv_charts_2\iv_filter_2\data_analytics_backend\test_data_nifty_july182024.csv",
            #                        index_col=False)
            # df_yest['ts'] = pd.to_datetime(df_yest['ts'])
            df_orig['prev'] = df_orig['ts'] < today
            logger.info(f'\n{symbol} {expiry} df orig prev is \n {df_orig}')
            # logger.info(f'type of ts is {type(df_yest["ts"])} and type of today is {type(today)}')
            # df_yest['prev'] = df_yest['ts'] < today
            # df_yest = df_yest[df_yest['prev'] == True]
            # df1 = df_orig.copy()
            df_yest = df_orig[df_orig['prev']==True].copy()
            logger.info(f'\n {symbol} {expiry} df_yest is \n {df_yest}')
            # # logger.info(f'fetch straddle with 1 {symbol} {expiry}')
            # df_today = DBHandler.get_straddle_minima(symbol, expiry, start_from=today)
            # df_today['prev'] = df_today['ts'] < today
            # df2 = df_orig.copy()
            df_today = df_orig[df_orig['prev']==False].copy()
            logger.info(f'\n {symbol} {expiry} df_today is \n {df_today}')
            # df_orig = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
            # df_yest = df_orig[df_orig['ts'] < today].copy()
            # df_yest['prev'] = True
            # df_today = df_orig[df_orig['ts'] >= today].copy()
            # df_today['prev'] = False
        else:
            df_yest = pd.DataFrame()
            df_today = DBHandler.get_straddle_minima(symbol, expiry)
            df_today['prev'] = False
            # # logger.info(f'fetch straddle with 2 {symbol} {expiry}')
        # if self.use_otm_iv:
        #     df_yest['combined_iv'] = df_yest['otm_iv']
        #     df_today['combined_iv'] = df_today['otm_iv']

        # # logger.info(f'\ndf of {symbol} {expiry} fetched from db is \n {df}')

        # if symbol == 'NIFTY' and expiry == '2024-07-18':
        #     df = pd.read_csv(r"D:\iv_charts_2\iv_filter_2\data_analytics_backend\test_time_data_nifty_cw_1672024.csv", index = False)
        #     fixed_df = pd.read_csv(r"D:\iv_charts_2\iv_filter_2\data_analytics_backend\test_time_data_nifty_cw_1672024.csv",
        #                      index=False)

        fixed_resp = fixed_response_dict()
        fixed_df = pd.DataFrame(fixed_resp)
        # # logger.info(f'\nfixed df is \n{fixed_df.head()}')
        # df_yest['ts'] = pd.to_datetime(df_yest['ts'])
        df_today['ts'] = pd.to_datetime(df_today['ts'])
        fixed_df['ts'] = pd.to_datetime(fixed_df['ts'])
        # logger.info(f'changed ts in all 3 df')

        # # mtd-1
        merged_df = pd.merge(fixed_df, df_today, on='ts', how='left', suffixes = ('', '_y'))
        logger.info(f'\n{symbol} {expiry} orig merged df is \n{merged_df}')
        # merged_df['strike'] = merged_df['strike'].fillna(0).astype(int)
        # # logger.info(f'merged df after strike is \n {merged_df}')
        # merged_df['combined_premium'] = merged_df['combined_premium'].fillna(0).astype(int)
        # # logger.info(f'merged df after combined_premium is \n {merged_df}')
        # Merge today's data with the fixed response dict
        # merged_today_df = pd.merge(fixed_df, df_today, on='ts', how='left', suffixes=('_x', '_y'))
        # Fill missing values
        col_list = ['spot', 'strike', 'combined_premium', 'combined_iv', 'otm_iv', 'prev']
        for col in col_list:
            merged_df[col] = merged_df[col].fillna(merged_df[f'{col}_y']).fillna(0)
        logger.info(f'\n{symbol} {expiry} merged df 1 is \n{merged_df}')
        merged_df.drop(columns=col_list, axis=1, inplace=True)
        logger.info(f'\n{symbol} {expiry} merged df 2 is \n{merged_df}')
        rename_dict = {f'{col}_y': col for col in col_list}
        merged_df.rename(columns=rename_dict, inplace=True)
        merged_df.fillna(0, inplace=True)
        merged_df['prev'] = False
        # merged_df = merged_df.apply(lambda col: col.fillna(0, inplace=True) if col.name != 'prev' else col)


        # merged_df = merged_df[['ts', 'spot', 'strike', 'combined_premium', 'combined_iv', 'otm_iv', 'prev']]
        if symbol == 'BANKNIFTY' and expiry == pd.to_datetime("2024-07-24").date():
            merged_df.to_csv(f'merged_df_try_bn_cw.csv', index = False)
        logger.info(f'\n{symbol} {expiry} updated merged df is \n{merged_df}')

        # mtd-2
        # fixed_df.set_index('ts')
        # # logger.info(f'fixed df {symbol} {expiry} is {fixed_df}')
        # df_today.set_index('ts')
        # # logger.info(f'df today {symbol} {expiry} is {df_today}')
        # df_yest.set_index('ts')
        # # logger.info(f'df yest {symbol} {expiry} is {df_yest}')
        # fixed_df.update(df_today[['spot','strike', 'combined_premium','combined_iv', 'otm_iv', 'prev']])
        # merged_df = fixed_df.copy()
        # logger.info(f'\n{symbol} {expiry} merged df is \n{merged_df}')
        final_df = pd.concat([df_yest, merged_df], axis=0)
        # # final_df[[]].fillna(0, inplace=True)
        # # final_df = final_df.apply(lambda col: col.fillna(0) if col.name != 'prev' else col)
        # # df_yest.update(merged_df[['spot','strike', 'combined_premium','combined_iv', 'otm_iv']])
        # # final_df = df_yest.copy()
        logger.info(f'\n{symbol} {expiry} final df is \n{final_df}')

        # # logger.info(f'merged df {symbol} {expiry} after updation is \n{final_df}')
        final_df.reset_index(drop=True, inplace=True)
        # logger.info(f'\nfinal df {symbol} {expiry} is \n{final_df.head()}')
        # logger.info(f'\ntype of expiry is {type(expiry)} and type of arg is {type(pd.to_datetime("2024-07-18").date())} test is {expiry==(pd.to_datetime("2024-07-18").date())}')
        # if symbol == 'NIFTY' and expiry == pd.to_datetime("2024-07-18").date():
        #     final_df.to_csv('test_data_nifty_july182024_new.csv', index=False)
        #     logger.info('csv file made successfully')

        # final_df = pd.read_csv(r"D:\iv_charts_2\iv_filter_2\data_analytics_backend\test_data_nifty_july182024_new.csv", index_col = False)
        # final_df['ts'] = pd.to_datetime(final_df['ts'])
        if self.use_otm_iv:
            final_df['combined_iv'] = final_df['otm_iv']
        # logger.info(f'\nmerged_df {symbol} {expiry} is \n {final_df}')
        return self._straddle_response(final_df, count=st_cnt, interval=interval)

    def fetch_straddle_minima_table(self, st_cnt: int = Query(default=None), interval: int = Query(1),
                                    cont: bool = Query(False), table: bool = Query(True)):
        if self.copy_symbol_expiry_map:
            # # logger.info(f'\nsym exp map is {self.copy_symbol_expiry_map}')
            for_table = []
            current_time = datetime.now().time()
            if current_time > time(9,15):
                for i in range(len(self.copy_symbol_expiry_map)):
                    #     print(f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
                    # # logger.info(
                    #     f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
                    name = self.copy_symbol_expiry_map[i]['name']
                    sorted_exp = sorted(self.copy_symbol_expiry_map[i]['expiry'])
                    if name == 'NIFTY':
                        new_exp = sorted_exp[:2]
                        dict_1 = {'NIFTY_CW': new_exp[0], 'NIFTY_NW': new_exp[1]}
                        for_table.append(dict_1)
                    elif name == 'BANKNIFTY':
                        new_exp = sorted_exp[:2]
                        dict_1 = {'BANKNIFTY_CW': new_exp[0], 'BANKNIFTY_NW': new_exp[1]}
                        for_table.append(dict_1)
                    elif name == 'FINNIFTY':
                        dict_1 = {'FINNIFTY': sorted_exp[0]}
                        for_table.append(dict_1)
                    else:
                        new_exp = sorted_exp[0]
                        dict_1 = {'MIDCPNIFTY': new_exp}
                        for_table.append(dict_1)
                # print('\n for table dict is ', for_table)
                # # logger.info(f'\nfor_table dict is {for_table}')

                final_json = []
                for i in for_table:
                    for symbol, expiry in i.items():
                        # print(f'\n key{count} is {key} and value{count} is {value}')
                        # # logger.info((f'\n original key is {symbol} and value is {expiry}'))
                        if symbol.startswith('NIFTY'):
                            symbol1 = 'NIFTY'
                        elif symbol.startswith('BANK'):
                            symbol1 = 'BANKNIFTY'
                        elif symbol.startswith('FIN'):
                            symbol1 = 'FINNIFTY'
                        else:
                            symbol1 = 'MIDCPNIFTY'
                        # # logger.info(f'\n changed key is {symbol1} and value is {expiry}')
                        list_dict_resp = DBHandler.get_straddle_minima_table(symbol1, expiry)
                        # # logger.info(f'\nlist_dict_resp is {list_dict_resp}')
                        # if symbol.startswith('NIFTY_CW'):
                        #     symbol2 = 'NF CW'
                        # elif symbol.startswith('BANKNIFTY_CW'):
                        #     symbol2 = 'BN CW'
                        # elif symbol.startswith('FINNIFTY'):
                        #     symbol2 = 'FN CW'
                        # elif symbol.startswith('MIDCPNIFTY'):
                        #     symbol2 = 'MN CW'
                        # elif symbol.startswith('NIFTY_NW'):
                        #     symbol2 = 'NF NW'
                        # elif symbol.startswith('BANKNIFTY_NW'):
                        #     symbol2 = 'BN NW'
                        new_dict = {symbol: list_dict_resp}
                        # # logger.info(f'\nnew_dict is {new_dict}')
                        final_json.append(new_dict)
                        # # logger.info(f'\nmaking final json resp- {final_json}')

                # # logger.info(f'\nFINAL JSON RESP IS {final_json}')
                # df_json = df.to_json()
                # # logger.info(f'\n df to json is {df_json}')
                # df_json = df.to_dict('records')
                # self.copy_symbol_expiry_map = None
                return final_json
            else:
                # return None
                empty_json = [
                        {
                            "BANKNIFTY_CW": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "BANKNIFTY_NW": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "FINNIFTY": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "MIDCPNIFTY": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "NIFTY_CW": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "NIFTY_NW": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        }
                    ]
                return empty_json

    def fetch_straddle_iv(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
                          interval: int = Query(5)):
        df = DBHandler.get_straddle_iv_data(symbol, expiry)
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        return self._straddle_response(df, count=st_cnt, interval=interval)

    # def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=15),
    #                            interval: int = Query(5)):
    #     all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday)
    #     all_data = []
    #     today_df = all_df[all_df['ts'] >= today].copy()
    #     prev_df = all_df[all_df['ts'] < today].copy()
    #     if len(prev_df):
    #         max_ts = prev_df['ts'].max()
    #         prev_df = prev_df[prev_df['ts'] == max_ts].copy()
    #         all_data.append(prev_df)
    #     if len(today_df):
    #         all_data.append(today_df)
    #
    #     if all_data:
    #         df = pd.concat(all_data, ignore_index=True, sort=False)
    #     else:
    #         df = all_df.iloc[:0]
    #     if self.use_otm_iv:
    #         df['combined_iv'] = df['otm_iv']
    #     # allowed = pd.date_range(df['ts'].min(), df['ts'].max(), freq=interval)
    #     # req = df[df['ts'].isin(allowed)].copy()
    #     break_ts = time(12, 30, 0)
    #     req1 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
    #     req1 = req1[req1['ts'].dt.time <= break_ts].copy()
    #     req2 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
    #     req2 = req2[req2['ts'].dt.time > break_ts].copy()  # prev day covered here
    #     d = [req1, req2]
    #     # d = [req1]
    #     d = [_d for _d in d if len(d)]
    #     if d:
    #         req = pd.concat(d, ignore_index=True, sort=False)
    #         req.sort_values(['ts', 'strike'], inplace=True)
    #     else:
    #         req = pd.DataFrame(columns=req1.columns)
    #     req = req.replace({np.NAN: None}).round(2)
    #     strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
    #     strike_iv.sort_values(['strike'], inplace=True)
    #     strikes = strike_iv['strike'].tolist()
    #     iv = list(zip(*strike_iv['combined_iv'].tolist()))
    #     ts = list(zip(*strike_iv['ts'].tolist()))
    #     return {'strikes': strikes, 'iv': iv, 'ts': ts}

    # def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=15),
    #                            interval: int = Query(5)):
    #     all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday)
    #     logger.info(f'\nall_df fetched from query for {symbol} {expiry} is \n {all_df}')
    #     all_data = []
    #     today_df = all_df[all_df['ts'] >= today].copy()
    #     # # logger.info(f'\ntoday df for {symbol} {expiry} is \n {today_df}')
    #     prev_df = all_df[all_df['ts'] < today].copy()
    #     # # logger.info(f'\nprev df for {symbol} {expiry} is \n {prev_df}')
    #     if len(prev_df):
    #         max_ts = prev_df['ts'].max()
    #         prev_df = prev_df[prev_df['ts'] == max_ts].copy()
    #         all_data.append(prev_df)
    #     if len(today_df):
    #         all_data.append(today_df)
    #
    #     # # logger.info(f'\nall data before append for {symbol} {expiry} is \n {all_data}')
    #     if all_data:
    #         df = pd.concat(all_data, ignore_index=True, sort=False)
    #     else:
    #         df = all_df.iloc[:0]
    #     # # logger.info(f'\nall data after append for {symbol} {expiry} is \n {all_data}')
    #     # # logger.info(f'\ndf data after append for {symbol} {expiry} is \n {df}')
    #     if self.use_otm_iv:
    #         df['combined_iv'] = df['otm_iv']
    #     # allowed = pd.date_range(df['ts'].min(), df['ts'].max(), freq=interval)
    #     # req = df[df['ts'].isin(allowed)].copy()
    #     break_ts = time(12, 30, 0)
    #     # req1 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
    #     # req1 = req1[req1['ts'].dt.time <= break_ts].copy()
    #     # req2 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
    #     # req2 = req2[req2['ts'].dt.time > break_ts].copy()  # prev day covered here
    #     # d = [req1, req2]
    #     # d = [_d for _d in d if len(d)]
    #     # if d:
    #     #     req = pd.concat(d, ignore_index=True, sort=False)
    #     #     req.sort_values(['ts', 'strike'], inplace=True)
    #     # # logger.info(f'\n df being sent to straddle response is \n{df}')
    #     req = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
    #     if req is not None:
    #         req.sort_values(['ts', 'strike'], inplace=True)
    #     else:
    #         # req = pd.DataFrame(columns=req1.columns)
    #         req = pd.DataFrame(columns=req.columns)
    #     req = req.replace({np.NAN: None}).round(2)
    #     strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
    #     strike_iv.sort_values(['strike'], inplace=True)
    #     strikes = strike_iv['strike'].tolist()
    #     iv = list(zip(*strike_iv['combined_iv'].tolist()))
    #     ts = list(zip(*strike_iv['ts'].tolist()))
    #     logger.info(f'\nfetch straddle cluster Response for {symbol} {expiry}: \nstrikes={strikes}, \niv={iv}, \nts={ts}')
    #     return {'strikes': strikes, 'iv': iv, 'ts': ts}

    def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=15),
                               interval: int = Query(5)):
        all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday)
        all_data = []
        today_df = all_df[all_df['ts'] >= today].copy()
        prev_df = all_df[all_df['ts'] < today].copy()
        if len(prev_df):
            max_ts = prev_df['ts'].max()
            prev_df = prev_df[prev_df['ts'] == max_ts].copy()
            all_data.append(prev_df)
        if len(today_df):
            all_data.append(today_df)

        if all_data:
            df = pd.concat(all_data, ignore_index=True, sort=False)
        else:
            df = all_df.iloc[:0]
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        # allowed = pd.date_range(df['ts'].min(), df['ts'].max(), freq=interval)
        # req = df[df['ts'].isin(allowed)].copy()
        break_ts = time(12, 30, 0)
        # req1 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
        # req1 = req1[req1['ts'].dt.time <= break_ts].copy()
        # req2 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
        # req2 = req2[req2['ts'].dt.time > break_ts].copy()  # prev day covered here
        # d = [req1, req2]
        # d = [_d for _d in d if len(d)]
        # if d:
        #     req = pd.concat(d, ignore_index=True, sort=False)
        #     req.sort_values(['ts', 'strike'], inplace=True)
        # logger.info(f'Dataframe values for symbol {symbol} for expiry::>>{expiry} is ::>>{df}')
        # df.to_csv(f"{symbol}.csv")
        req = self._straddle_response(df, raw=True, count=st_cnt, interval=30)

        if req is not None:
            req.sort_values(['ts', 'strike'], inplace=True)
        else:
            # req = pd.DataFrame(columns=req1.columns)
            req = pd.DataFrame(columns=req.columns)

        req = req.replace({np.NAN: None}).round(2)
        # logger.info(f'Req body for symbol::{symbol} is  ::>> {req}')
        # logger.info(f'req values for symbol {symbol} for expiry::>>{expiry} is ::>>{req}')
        req.to_csv(f"{symbol}.csv")
        strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
        strike_iv.sort_values(['strike'], inplace=True)
        strikes = strike_iv['strike'].tolist()
        # iv = list(zip(*strike_iv['combined_iv'].tolist()))
        # ts = list(zip(*strike_iv['ts'].tolist()))
        # print(f'Strikes::>>>>{strikes} of symbol {symbol} of expiry {expiry}')
        # print(f'iv::>>>>{iv} of symbol {symbol} of expiry {expiry}')
        # print(f'ts::>>>>{ts} of symbol {symbol} of expiry {expiry}')
        # Pad both combined_iv and ts to handle varying lengths
        max_len = max(len(x) for x in strike_iv['ts'])  # chk if len(strikes) == len(strikes_iv['ts'])
        strike_iv['combined_iv'] = strike_iv['combined_iv'].apply(lambda x: x + [None] * (max_len - len(x)))
        strike_iv['ts'] = strike_iv['ts'].apply(lambda x: x + [None] * (max_len - len(x)))

        # iv = list(zip_longest(*strike_iv['combined_iv'].tolist(), fillvalue=None))
        # ts = list(zip_longest(*strike_iv['ts'].tolist(), fillvalue=None))

        # # Filter out None values from the aligned lists
        # # iv = [list(filter(lambda x: x is not None, ivs)) for ivs in iv]
        # ts = [list(filter(lambda x: x is not None, t)) for t in ts]

        # -----
        combined_iv_list = strike_iv['combined_iv'].tolist()
        for i in range(len(combined_iv_list[0])):
            for j in range(len(strikes)):
                if combined_iv_list[j][i] is None:
                    lesser_iv = None
                    greater_iv = None
                    # Finding the lesser strike IV
                    for k in range(j - 1, -1, -1):
                        if combined_iv_list[k][i] is not None:
                            lesser_iv = combined_iv_list[k][i]
                            break
                    # Finding the greater strike IV
                    for k in range(j + 1, len(strikes)):
                        if combined_iv_list[k][i] is not None:
                            greater_iv = combined_iv_list[k][i]
                            break
                    if lesser_iv is not None and greater_iv is not None:
                        combined_iv_list[j][i] = (lesser_iv + greater_iv) / 2

        iv = list(zip_longest(*combined_iv_list, fillvalue=None))
        ts = list(zip_longest(*strike_iv['ts'].tolist(), fillvalue=None))

        ts = [list(filter(lambda x: x is not None, t)) for t in ts]

        return {'strikes': strikes, 'iv': iv, 'ts': ts}

    def _straddle_response(self, df: pd.DataFrame, raw=False, count: int = None, interval: int = None):
        count = 10 if count is None else count
        l_st, u_st = count + 1, count
        logger.info(f'\ndf spot is \n {df["spot"]}')
        df['spot'] = pd.to_numeric(df['spot'], errors='coerce')
        df = df.dropna(subset=['spot'])
        mean = df['spot'].mean()
        # # logger.info(f'\nmean is {mean}')
        uq_strikes = pd.to_numeric(df['strike'], errors='coerce').dropna().unique()
        # uq_strikes = df['strike'].unique()
        uq_strikes.sort()
        strikes = uq_strikes[uq_strikes <= mean][-l_st:].tolist() + uq_strikes[uq_strikes > mean][:u_st].tolist()
        logger.info(f'\n uq_strikes are \n{uq_strikes}, \n strikes are \n{strikes}')
        df: pd.DataFrame = df[df['strike'].isin(uq_strikes)].copy()
        # # logger.info(f'df before drop is \n {df}')
        df.drop(columns=['spot', 'range'], errors='ignore', inplace=True)
        df.sort_values(['ts', 'strike'], inplace=True)
        # # logger.info(f'df after drop is \n {df}')
        if interval and len(df):
            valid_ts = pd.date_range(start=df['ts'].min(), end=df['ts'].max(), freq=f'{interval}min')
            # logger.info(f'Total timestamps generated: {valid_ts}')
            if len(valid_ts):
                df = df[df['ts'].isin(valid_ts)].copy()
            # logger.info(f'Valid timestamps generated: {valid_ts}')
        # # logger.info(f'\ndf after valid ts is \n {df}')
        if raw:
            return df
        # # logger.info(f'\nstraddle response df is \n {df.head()}')
        return self.df_response(df, to_millis=['ts'])

    @staticmethod
    def df_response(df: pd.DataFrame, to_millis: list = None) -> list[dict]:
        df = df.replace({np.NAN: None}).round(2)
        dict1 = df.to_dict('records')
        # # logger.info(f'\ndf_response_dict1[0] before epoch conversion is {dict1[0]}')

        # converting local time to unix time
        if to_millis is not None and len(to_millis) and len(df):
            for _col in to_millis:
                df[_col] = (df[_col].dt.tz_localize(IST).astype('int64') // 10 ** 9) * 1000

        dict2 = df.to_dict('records')
        # # logger.info(f'\ndf_response_dict1[0] after epoch conversion is {dict2[0]}')
        # for _key, _value in dict1[0].items():
        #     print(f'\n1st line of dftodict is {_key}:{_value}')
        # count = 0
        # for _entity in dict1[0]:
        #     for _key, _value in _entity.items():
        #         # logger.info(f'\n1st line of dftodict is {_key}:{_value}')
        #         if count == 0:
        #             break
        logger.info(f'\nstraddle response df is {dict2}')
        return df.to_dict('records')
    # response is LIST OF DICTIONARIES
    # sample response = {"ts":1714384740000,"strike":22700.0,"combined_premium":190.1,"combined_iv":11.52,"otm_iv":11.52,"prev":false}


service = ServiceApp()
app = service.app

if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=8811, workers=5)
