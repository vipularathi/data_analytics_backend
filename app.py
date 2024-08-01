from datetime import date, datetime, time

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from common import IST, yesterday, today, logger
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

    def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
                              interval: int = Query(1), cont: bool = Query(False)):
        logger.info(f'inside fetch_straddle_minima for {symbol} {expiry} and cont is {cont}')
        if cont:
            df = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
            df['prev'] = df['ts'] < today
            logger.info(f'fetch straddle with 1 {symbol} {expiry}')
        else:
            df = DBHandler.get_straddle_minima(symbol, expiry)
            df['prev'] = False
            logger.info(f'fetch straddle with 2 {symbol} {expiry}')
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        logger.info(f'\nstraddle_minima df for {symbol} {expiry} is \n {df}')
        return self._straddle_response(df, count=st_cnt, interval=interval)


    # def fetch_straddle_minima_table(self, st_cnt: int = Query(default=None), interval: int = Query(1),
    #                                 cont: bool = Query(False), table: bool = Query(True)):
    #     if self.copy_symbol_expiry_map:
    #         logger.info(f'\nsym exp map is {self.copy_symbol_expiry_map}')
    #         for_table = []
    #         for i in range(len(self.copy_symbol_expiry_map)):
    #             #     print(f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
    #             logger.info(
    #                 f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
    #             name = self.copy_symbol_expiry_map[i]['name'];
    #             sorted_exp = sorted(self.copy_symbol_expiry_map[i]['expiry'])
    #             if name == 'NIFTY':
    #                 new_exp = sorted_exp[:2]
    #                 dict_1 = {'NIFTY_CW': new_exp[0], 'NIFTY_NW': new_exp[1]}
    #                 for_table.append(dict_1)
    #             elif name == 'BANKNIFTY':
    #                 new_exp = sorted_exp[:2]
    #                 dict_1 = {'BANKNIFTY_CW': new_exp[0], 'BANKNIFTY_NW': new_exp[1]}
    #                 for_table.append(dict_1)
    #             elif name == 'FINNIFTY':
    #                 dict_1 = {'FINNIFTY': sorted_exp[0]}
    #                 for_table.append(dict_1)
    #             else:
    #                 new_exp = sorted_exp[0]
    #                 dict_1 = {'MIDCPNIFTY': new_exp}
    #                 for_table.append(dict_1)
    #         # print('\n for table dict is ', for_table)
    #         logger.info(f'\nfor_table dict is {for_table}')

    #         final_json = []
    #         for i in for_table:
    #             for symbol, expiry in i.items():
    #                 # print(f'\n key{count} is {key} and value{count} is {value}')
    #                 logger.info((f'\n original key is {symbol} and value is {expiry}'))
    #                 if symbol.startswith('NIFTY'):
    #                     symbol1 = 'NIFTY'
    #                 elif symbol.startswith('BANK'):
    #                     symbol1 = 'BANKNIFTY'
    #                 elif symbol.startswith('FIN'):
    #                     symbol1 = 'FINNIFTY'
    #                 else:
    #                     symbol1 = 'MIDCPNIFTY'
    #                 logger.info(f'\n changed key is {symbol1} and value is {expiry}')
    #                 list_dict_resp = DBHandler.get_straddle_minima_table(symbol1, expiry)
    #                 logger.info(f'\nlist_dict_resp is {list_dict_resp}')
    #                 # if symbol.startswith('NIFTY_CW'):
    #                 #     symbol2 = 'NF CW'
    #                 # elif symbol.startswith('BANKNIFTY_CW'):
    #                 #     symbol2 = 'BN CW'
    #                 # elif symbol.startswith('FINNIFTY'):
    #                 #     symbol2 = 'FN CW'
    #                 # elif symbol.startswith('MIDCPNIFTY'):
    #                 #     symbol2 = 'MN CW'
    #                 # elif symbol.startswith('NIFTY_NW'):
    #                 #     symbol2 = 'NF NW'
    #                 # elif symbol.startswith('BANKNIFTY_NW'):
    #                 #     symbol2 = 'BN NW'
    #                 new_dict = {symbol: list_dict_resp}
    #                 logger.info(f'\nnew_dict is {new_dict}')
    #                 final_json.append(new_dict)
    #                 logger.info(f'\nmaking final json resp- {final_json}')

    #         logger.info(f'\nFINAL JSON RESP IS {final_json}')
    #         # df_json = df.to_json()
    #         # logger.info(f'\n df to json is {df_json}')
    #         # df_json = df.to_dict('records')
    #         # self.copy_symbol_expiry_map = None
    #         return final_json

    def fetch_straddle_minima_table(self, st_cnt: int = Query(default=None), interval: int = Query(1),
                                    cont: bool = Query(False), table: bool = Query(True)):
        if self.copy_symbol_expiry_map:
            # logger.info(f'\nsym exp map is {self.copy_symbol_expiry_map}')
            for_table = []
            current_time = datetime.now().time()
            # if current_time > time(9,15):
            #     for i in range(len(self.copy_symbol_expiry_map)):
            #         #     print(f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
            #         # logger.info(
            #         #     f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
            #         name = self.copy_symbol_expiry_map[i]['name']
            #         sorted_exp = sorted(self.copy_symbol_expiry_map[i]['expiry'])
            #         if name == 'NIFTY':
            #             new_exp = sorted_exp[:2]
            #             dict_1 = {'NIFTY_CW': new_exp[0], 'NIFTY_NW': new_exp[1]}
            #             for_table.append(dict_1)
            #         elif name == 'BANKNIFTY':
            #             new_exp = sorted_exp[:2]
            #             dict_1 = {'BANKNIFTY_CW': new_exp[0], 'BANKNIFTY_NW': new_exp[1]}
            #             for_table.append(dict_1)
            #         elif name == 'FINNIFTY':
            #             dict_1 = {'FINNIFTY': sorted_exp[0]}
            #             for_table.append(dict_1)
            #         else:
            #             new_exp = sorted_exp[0]
            #             dict_1 = {'MIDCPNIFTY': new_exp}
            #             for_table.append(dict_1)
            #     # print('\n for table dict is ', for_table)
            #     # logger.info(f'\nfor_table dict is {for_table}')
            #
            #     final_json = []
            #     for i in for_table:
            #         for symbol, expiry in i.items():
            #             # print(f'\n key{count} is {key} and value{count} is {value}')
            #             # logger.info((f'\n original key is {symbol} and value is {expiry}'))
            #             if symbol.startswith('NIFTY'):
            #                 symbol1 = 'NIFTY'
            #             elif symbol.startswith('BANK'):
            #                 symbol1 = 'BANKNIFTY'
            #             elif symbol.startswith('FIN'):
            #                 symbol1 = 'FINNIFTY'
            #             else:
            #                 symbol1 = 'MIDCPNIFTY'
            #             # logger.info(f'\n changed key is {symbol1} and value is {expiry}')
            #             list_dict_resp = DBHandler.get_straddle_minima_table(symbol1, expiry)
            #             # logger.info(f'\nlist_dict_resp is {list_dict_resp}')
            #             # if symbol.startswith('NIFTY_CW'):
            #             #     symbol2 = 'NF CW'
            #             # elif symbol.startswith('BANKNIFTY_CW'):
            #             #     symbol2 = 'BN CW'
            #             # elif symbol.startswith('FINNIFTY'):
            #             #     symbol2 = 'FN CW'
            #             # elif symbol.startswith('MIDCPNIFTY'):
            #             #     symbol2 = 'MN CW'
            #             # elif symbol.startswith('NIFTY_NW'):
            #             #     symbol2 = 'NF NW'
            #             # elif symbol.startswith('BANKNIFTY_NW'):
            #             #     symbol2 = 'BN NW'
            #             new_dict = {symbol: list_dict_resp}
            #             # logger.info(f'\nnew_dict is {new_dict}')
            #             final_json.append(new_dict)
            #             # logger.info(f'\nmaking final json resp- {final_json}')
            #
            #     # logger.info(f'\nFINAL JSON RESP IS {final_json}')
            #     # df_json = df.to_json()
            #     # logger.info(f'\n df to json is {df_json}')
            #     # df_json = df.to_dict('records')
            #     # self.copy_symbol_expiry_map = None
            #     return final_json
            # else:
            #     # return None
            #     empty_json = [
            #             {
            #                 "BANKNIFTY_CW": [
            #                     {
            #                         "Live": 0,
            #                         "Live-Min": 0,
            #                         "Max-Live": 0,
            #                         "Max": 0,
            #                         "Min": 0
            #                     }
            #                 ]
            #             },
            #             {
            #                 "BANKNIFTY_NW": [
            #                     {
            #                         "Live": 0,
            #                         "Live-Min": 0,
            #                         "Max-Live": 0,
            #                         "Max": 0,
            #                         "Min": 0
            #                     }
            #                 ]
            #             },
            #             {
            #                 "FINNIFTY": [
            #                     {
            #                         "Live": 0,
            #                         "Live-Min": 0,
            #                         "Max-Live": 0,
            #                         "Max": 0,
            #                         "Min": 0
            #                     }
            #                 ]
            #             },
            #             {
            #                 "MIDCPNIFTY": [
            #                     {
            #                         "Live": 0,
            #                         "Live-Min": 0,
            #                         "Max-Live": 0,
            #                         "Max": 0,
            #                         "Min": 0
            #                     }
            #                 ]
            #             },
            #             {
            #                 "NIFTY_CW": [
            #                     {
            #                         "Live": 0,
            #                         "Live-Min": 0,
            #                         "Max-Live": 0,
            #                         "Max": 0,
            #                         "Min": 0
            #                     }
            #                 ]
            #             },
            #             {
            #                 "NIFTY_NW": [
            #                     {
            #                         "Live": 0,
            #                         "Live-Min": 0,
            #                         "Max-Live": 0,
            #                         "Max": 0,
            #                         "Min": 0
            #                     }
            #                 ]
            #             }
            #         ]
            #     return empty_json
            for i in range(len(self.copy_symbol_expiry_map)):
                #     print(f'\n expiry of each symbol is {self.copy_symbol_expiry_map[i]["name"]} {sorted(self.copy_symbol_expiry_map[i]["expiry"])}')
                # logger.info(
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
            # logger.info(f'\nfor_table dict is {for_table}')

            final_json = []
            for i in for_table:
                for symbol, expiry in i.items():
                    # print(f'\n key{count} is {key} and value{count} is {value}')
                    # logger.info((f'\n original key is {symbol} and value is {expiry}'))
                    if symbol.startswith('NIFTY'):
                        symbol1 = 'NIFTY'
                    elif symbol.startswith('BANK'):
                        symbol1 = 'BANKNIFTY'
                    elif symbol.startswith('FIN'):
                        symbol1 = 'FINNIFTY'
                    else:
                        symbol1 = 'MIDCPNIFTY'
                    # logger.info(f'\n changed key is {symbol1} and value is {expiry}')
                    list_dict_resp = DBHandler.get_straddle_minima_table(symbol1, expiry)
                    # logger.info(f'\nlist_dict_resp is {list_dict_resp}')
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
                    # logger.info(f'\nnew_dict is {new_dict}')
                    final_json.append(new_dict)
                    # logger.info(f'\nmaking final json resp- {final_json}')

            # logger.info(f'\nFINAL JSON RESP IS {final_json}')
            # df_json = df.to_json()
            # logger.info(f'\n df to json is {df_json}')
            # df_json = df.to_dict('records')
            # self.copy_symbol_expiry_map = None
            return final_json

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
        req = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
        if req is not None:
            req.sort_values(['ts', 'strike'], inplace=True)
        else:
            req = pd.DataFrame(columns=req.columns)
        req = req.replace({np.NAN: None}).round(2)
        strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
        strike_iv.sort_values(['strike'], inplace=True)
        strikes = strike_iv['strike'].tolist()
        iv = list(zip(*strike_iv['combined_iv'].tolist()))
        ts = list(zip(*strike_iv['ts'].tolist()))
        return {'strikes': strikes, 'iv': iv, 'ts': ts}

    def _straddle_response(self, df: pd.DataFrame, raw=False, count: int = None, interval: int = None):
        count = 10 if count is None else count
        l_st, u_st = count + 1, count
        # df['range'] = (df['spot'] - df['strike']).abs() < (df['spot'] * 0.05)
        # strikes = df[df['range']]['strike'].unique()
        mean = df['spot'].mean()
        uq_strikes = df['strike'].unique()
        uq_strikes.sort()
        strikes = uq_strikes[uq_strikes <= mean][-l_st:].tolist() + uq_strikes[uq_strikes > mean][:u_st].tolist()
        # print(uq_strikes, strikes)
        df: pd.DataFrame = df[df['strike'].isin(strikes)].copy()
        df.drop(columns=['spot', 'range'], errors='ignore', inplace=True)
        df.sort_values(['ts', 'strike'], inplace=True)
        if interval and len(df):
            valid_ts = pd.date_range(start=df['ts'].min(), end=df['ts'].max(), freq=f'{interval}min')
            if len(valid_ts):
                df = df[df['ts'].isin(valid_ts)].copy()
        if raw:
            return df
        logger.info(f'straddle response df is \n {df.head()}')
        return self.df_response(df, to_millis=['ts'])

    @staticmethod
    def df_response(df: pd.DataFrame, to_millis: list = None) -> list[dict]:
        df = df.replace({np.NAN: None}).round(2)
        dict1 = df.to_dict('records')
        logger.info(f'\ndf_response_dict1[0] before epoch conversion is {dict1[0]}')

        # converting local time to unix time
        if to_millis is not None and len(to_millis) and len(df):
            for _col in to_millis:
                df[_col] = (df[_col].dt.tz_localize(IST).astype('int64') // 10 ** 9) * 1000

        dict2 = df.to_dict('records')
        logger.info(f'\ndf_response_dict1[0] after epoch conversion is {dict2[0]}')
        # for _key, _value in dict1[0].items():
        #     print(f'\n1st line of dftodict is {_key}:{_value}')
        # count = 0
        # for _entity in dict1[0]:
        #     for _key, _value in _entity.items():
        #         logger.info(f'\n1st line of dftodict is {_key}:{_value}')
        #         if count == 0:
        #             break
        # logger.info(f'dict1 is {dict1}')
        return df.to_dict('records')
    # response is LIST OF DICTIONARIES
    # sample response = {"ts":1714384740000,"strike":22700.0,"combined_premium":190.1,"combined_iv":11.52,"otm_iv":11.52,"prev":false}


service = ServiceApp()
app = service.app

if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=8801, workers=2)
