from datetime import date, datetime, time
from itertools import zip_longest
import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pytz
from pydantic import BaseModel
from passlib.context import CryptContext
import os
import asyncio
from scipy.interpolate import interp1d
from common import IST, yesterday, today, logger, fixed_response_dict, round_spot, read_symbols, trading_time_range, start_time, end_time, data_dir
from contracts import get_req_contracts
from db_ops import DBHandler, insert_data_df
from remote_db_ops import check_fill_data_parallel


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
        self.copy_symbol_expiry_spot = None
        self.list_dict = []
        self.difference = None
        self.use_strikes = []
        self.strd_clst_nnm = None

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

    def get_symbols(self):
        if self.symbol_expiry_map is None:
            ins_df, tokens, token_xref = get_req_contracts()
            ins_df['expiry'] = ins_df['expiry'].dt.strftime('%Y-%m-%d')
            agg = ins_df[ins_df['instrument_type'].isin(['CE', 'PE'])].groupby(['name'], as_index=False).agg(
                {'expiry': set, 'tradingsymbol': 'count'})
            agg['expiry'] = agg['expiry'].apply(lambda x: sorted(list(x)))
            self.symbol_expiry_map = agg.to_dict('records')
            symbol_expiry_map = agg.to_dict('records')
            logger.info(f'\n symbol expiry map is \n{symbol_expiry_map} \n and type is {type(symbol_expiry_map)}')
            self.copy_symbol_expiry_map = self.symbol_expiry_map.copy()
        # current_minute = pd.Timestamp.now().replace(second=0, microsecond=0)
        # # logger.info(f'\n type current minute is {type(current_minute)} {current_minute.tzinfo}\n type start time is {type(start_time)} {start_time.tzinfo}\n type end time is {type(end_time)} {end_time.tzinfo}')
        # if start_time <= current_minute <= end_time:
        #     loc = trading_time_range.index(current_minute)
        #     extracted_time_range = trading_time_range[:loc + 1]
        #     asyncio.run(check_fill_data_parallel([extracted_time_range]))
        # elif current_minute > end_time:
        #     asyncio.run(check_fill_data_parallel([trading_time_range]))
        return self.symbol_expiry_map

    def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
                              interval: int = Query(1), cont: bool = Query(False)):
        # logger.info(f'{symbol} {expiry} and cont is {cont}')
        local_today = pd.Timestamp(today).tz_localize('Asia/Kolkata')
        if cont:
            df_orig = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
            logger.info(f'\n{symbol} {expiry} {cont} {today} {yesterday} df orig is \n {df_orig}')

            # ----------------------------------------------------------------
            # df_orig['ts'] = df_orig['ts'].dt.tz_localize('Asia/Kolkata')
            # logger.info(f'df_orig timezone is {df_orig["ts"].dtype}')
            # ----------------------------------------------------------------

            df_orig['prev'] = df_orig['ts'] < today
            logger.info(f'')
            logger.info(f'\n{symbol} {expiry} df orig prev is \n {df_orig}')

            df_yest = df_orig[df_orig['prev']==True].copy()
            logger.info(f'\n {symbol} {expiry} df_yest is \n {df_yest}')

            df_today = df_orig[df_orig['prev']==False].copy()
            logger.info(f'\n {symbol} {expiry} df_today is \n {df_today}')

        else:
            df_yest = pd.DataFrame()
            df_today = DBHandler.get_straddle_minima(symbol, expiry)
            df_today['prev'] = False

        # df_today['ts'] = df_today['ts'].dt.tz_convert(None)


        fixed_resp = fixed_response_dict()
        fixed_df = pd.DataFrame(fixed_resp)
        df_today['ts'] = pd.to_datetime(df_today['ts'])
        fixed_df['ts'] = pd.to_datetime(fixed_df['ts'])
        merged_df = pd.merge(fixed_df, df_today, on='ts', how='left', suffixes = ('', '_y'))
        # logger.info(f'\n{symbol} {expiry} orig merged df is \n{merged_df}')

        # Fill missing values
        col_list = ['spot', 'strike', 'combined_premium', 'combined_iv', 'otm_iv', 'prev']
        for col in col_list:
            merged_df[col] = merged_df[col].fillna(merged_df[f'{col}_y']).fillna(0)
        # logger.info(f'\n{symbol} {expiry} merged df 1 is \n{merged_df}')
        merged_df.drop(columns=col_list, axis=1, inplace=True)
        # logger.info(f'\n{symbol} {expiry} merged df 2 is \n{merged_df}')
        rename_dict = {f'{col}_y': col for col in col_list}
        merged_df.rename(columns=rename_dict, inplace=True)
        merged_df.fillna(0, inplace=True)
        merged_df['prev'] = False
        # logger.info(f'\n{symbol} {expiry} updated merged df is \n{merged_df}')

        final_df = pd.concat([df_yest, merged_df], axis=0)
        # logger.info(f'\n{symbol} {expiry} final df is \n{final_df}')
        # # logger.info(f'merged df {symbol} {expiry} after updation is \n{final_df}')
        final_df.reset_index(drop=True, inplace=True)

        if self.use_otm_iv:
            final_df['combined_iv'] = final_df['otm_iv']
        return self._straddle_response(final_df, count=st_cnt, interval=interval, sym= symbol, exp=str(expiry))

    def fetch_straddle_minima_table(self, st_cnt: int = Query(default=None), interval: int = Query(1),
                                    cont: bool = Query(False), table: bool = Query(True)):
        if self.copy_symbol_expiry_map:
            # # logger.info(f'\nsym exp map is {self.copy_symbol_expiry_map}')
            for_table = []
            current_time = datetime.now().time()
            if current_time > time(9,15):
                for i in range(len(self.copy_symbol_expiry_map)):
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

                logger.info(f'\n for table dict is \n{for_table}')

                final_json = []
                for i in for_table:
                    for symbol, expiry in i.items():
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
                        new_dict = {symbol: list_dict_resp}
                        # # logger.info(f'\nnew_dict is {new_dict}')
                        final_json.append(new_dict)
                        # # logger.info(f'\nmaking final json resp- {final_json}')
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
                          interval: int = Query(1)):
        df = DBHandler.get_straddle_iv_data(symbol, expiry)
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        return self._straddle_response(df, count=st_cnt, interval=interval)

    def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=10),
                               interval: int = Query(5)):
        list_exp = read_symbols['expiry'][read_symbols['symbol'] == 'NIFTY'].tolist()
        logger.info(f'\n {symbol} {expiry} list_exp is {list_exp}')
        if pd.to_datetime(expiry) == pd.to_datetime(list_exp[-1]):
            all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday, strd_clst_nnm = True)
        else:
            all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday, strd_clst_nnm=True)
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
        # ----------------------------------------------------------------
        # all_df['ts'] = all_df['ts'].dt.tz_localize('Asia/Kolkata')
        # logger.info(f'all_df timezone is {all_df["ts"].dtype}')
        # ----------------------------------------------------------------
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        today_max_ts = df['ts'].unique().max()
        spot_today_max_ts = df['spot'][df['ts'] == today_max_ts].unique().tolist()

        logger.info(f'\n {symbol} {expiry} today max ts is {today_max_ts}\t type is {type(today_max_ts)}')
        logger.info(f'\n {symbol} {expiry} spot today max ts is {spot_today_max_ts}\t type is {type(spot_today_max_ts)}')
        # list_exp = read_symbols['expiry'][read_symbols['symbol'] == 'NIFTY'].tolist()
        # logger.info(f'\n {symbol} {expiry} list_exp is {list_exp}')

        if symbol == 'NIFTY' or symbol == 'FINNIFTY':
            if pd.to_datetime(expiry) == pd.to_datetime(list_exp[-1]):
                self.difference = 100
                self.strd_clst_nnm = True
                # df.to_csv(os.path.join(data_dir, 'nifty_NM_orig_df_fetched.csv'), index=False)
            else:
                self.difference = 50
                self.strd_clst_nnm = False
        elif symbol == 'BANKNIFTY':
            self.difference = 100
            self.strd_clst_nnm = False
        elif symbol == 'MIDCPNIFTY':
            self.difference = 25
            self.strd_clst_nnm = False
        # logger.info(f'\n list exp is {list_exp}\n list exp last is {list_exp[-1]} and type is {type(list_exp[-1])}')
        rounded_spot = round_spot(symbol=symbol, spot_multiple=self.difference, spot=spot_today_max_ts[0])
        logger.info(f"\n{symbol} {expiry} rounded spot is {rounded_spot} \t type is {type(rounded_spot)}")

        self.list_dict.append({symbol: [expiry, self.difference]})
        req = self._straddle_response(df, raw=True, count=st_cnt, interval=30, spot = rounded_spot, diff = self.difference, sym=symbol, exp = str(expiry), strd_clst_nnm = self.strd_clst_nnm, clst=True)

        if req is not None:
            req.sort_values(['ts', 'strike'], inplace=True)
        else:
            req = pd.DataFrame(columns=req.columns)

        req = req.replace({np.NAN: None}).round(2)
        strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
        strike_iv.sort_values(['strike'], inplace=True)
        strikes = strike_iv['strike'].tolist()
        max_len = max(len(x) for x in strike_iv['ts'])  # chk if len(strikes) == len(strikes_iv['ts'])
        strike_iv['combined_iv'] = strike_iv['combined_iv'].apply(lambda x: x + [None] * (max_len - len(x)))
        strike_iv['ts'] = strike_iv['ts'].apply(lambda x: x + [None] * (max_len - len(x)))

        # -----
        combined_iv_list = strike_iv['combined_iv'].tolist()

        # # interpolation
        # for i in range(len(combined_iv_list[0])):
        #     for j in range(len(strikes)):
        #         if combined_iv_list[j][i] is None:
        #             lesser_iv = None
        #             greater_iv = None
        #             # Finding the lesser strike IV
        #             for k in range(j - 1, -1, -1):
        #                 if combined_iv_list[k][i] is not None:
        #                     lesser_iv = combined_iv_list[k][i]
        #                     break
        #             # Finding the greater strike IV
        #             for k in range(j + 1, len(strikes)):
        #                 if combined_iv_list[k][i] is not None:
        #                     greater_iv = combined_iv_list[k][i]
        #                     break
        #             if lesser_iv is not None and greater_iv is not None:
        #                 combined_iv_list[j][i] = (lesser_iv + greater_iv) / 2

        # ---- Linear Interpolation to Fill None Values ----
        # def interpolate_iv(iv_list):
        #     indices = [i for i, v in enumerate(iv_list) if v is not None]
        #     if len(indices) >= 2:  # We need at least two points to interpolate
        #         interp_func = interp1d(indices, [iv_list[i] for i in indices], kind='linear', fill_value="extrapolate")
        #         for i in range(len(iv_list)):
        #             if iv_list[i] is None:
        #                 iv_list[i] = float(interp_func(i))  # Fill with interpolated value
        #     return iv_list
        #
        # # Apply interpolation to each IV series in the combined_iv_list
        # combined_iv_list = [interpolate_iv(iv) for iv in combined_iv_list]

        # # ---- Fill None Values by Connecting Non-None Values with a Straight Line ----
        # def fill_gaps_with_line(iv_list):
        #     start_idx = None
        #     for i, value in enumerate(iv_list):
        #         if value is not None:
        #             if start_idx is not None:
        #                 # Calculate the slope (m) and intercept (c) for the line connecting start_idx and i
        #                 start_value = iv_list[start_idx]
        #                 slope = (value - start_value) / (i - start_idx)
        #                 # Fill the values between start_idx and i
        #                 for j in range(start_idx + 1, i):
        #                     iv_list[j] = start_value + slope * (j - start_idx)
        #             start_idx = i  # Update the start index to the current non-None value
        #     return iv_list
        #
        # # Apply the line filling method to each IV series in the combined_iv_list
        # combined_iv_list = [fill_gaps_with_line(iv) for iv in combined_iv_list]

        # # ---- Fill None Values with Interpolation Between Non-None Values ----
        # def fill_gaps(iv_list):
        #     n = len(iv_list)
        #     i = 0
        #     while i < n:
        #         if iv_list[i] is None:
        #             # Find the previous non-None value
        #             prev_idx = i - 1
        #             while prev_idx >= 0 and iv_list[prev_idx] is None:
        #                 prev_idx -= 1
        #
        #             # Find the next non-None value
        #             next_idx = i + 1
        #             while next_idx < n and iv_list[next_idx] is None:
        #                 next_idx += 1
        #
        #             if prev_idx >= 0 and next_idx < n:
        #                 prev_value = iv_list[prev_idx]
        #                 next_value = iv_list[next_idx]
        #                 gap_length = next_idx - prev_idx
        #
        #                 # Calculate the slope (m)
        #                 slope = (next_value - prev_value) / gap_length
        #
        #                 # Fill the gap with the values on the line
        #                 for j in range(1, next_idx - prev_idx):
        #                     iv_list[prev_idx + j] = prev_value + slope * j
        #
        #             i = next_idx
        #         else:
        #             i += 1
        #     return iv_list
        #
        # # Apply the gap-filling method to each IV series in the combined_iv_list
        # combined_iv_list = [fill_gaps(iv) for iv in combined_iv_list]
        
        # interpolation using np.interp()
        combined_iv_list = strike_iv['combined_iv'].tolist()
        for i in range(len(combined_iv_list[0])):
            ivs = [combined_iv_list[j][i] for j in range(len(strikes))]
            mask = [v is not None for v in ivs]  # Mask to identify non-None values

            # Get the strike values and corresponding IVs where IV is not None
            valid_strikes = np.array([strike for strike, valid in zip(strikes, mask) if valid])
            valid_ivs = np.array([iv for iv in ivs if iv is not None])

            # Interpolate missing IVs
            interpolated_ivs = np.interp(strikes, valid_strikes, valid_ivs)

            # Assign the interpolated values back to combined_iv_list
            for j in range(len(strikes)):
                if not mask[j]:  # If original IV was None, replace it with interpolated value
                    combined_iv_list[j][i] = interpolated_ivs[j]


        logger.info(f'\n{symbol} {expiry} type of combined_iv_list is {type(combined_iv_list)} \nand combined_v_list is \n{combined_iv_list}')
        # iv = list(zip_longest(*combined_iv_list, fillvalue=None))
        # ts = list(zip_longest(*strike_iv['ts'].tolist(), fillvalue=None))
        iv = list(zip(*combined_iv_list))
        ts = list(zip(*strike_iv['ts'].tolist()))
        ts = [list(filter(lambda x: x is not None, t)) for t in ts]

        logger.info(f'\n{symbol} {expiry} type of iv is {type(iv)}')
        a = {'strikes': strikes, 'iv': iv, 'ts': ts, 'spot': [rounded_spot]}
        logger.info(f'\n{symbol} {expiry} a is \n{a}')

        return {'strikes': strikes, 'iv': iv, 'ts': ts, 'spot': [rounded_spot], 'symbol': [symbol], 'expiry': [expiry]}

    def _straddle_response(self, df: pd.DataFrame, raw=False, count: int = None, interval: int = None, spot: int = None, diff: int = None, sym: str=None, exp:str=None, strd_clst_nnm:bool=False, clst:str=False):
        if diff:
            if strd_clst_nnm:
                count = 7
            else:
                count = 10
            up_strike = [spot + i * diff for i in range(count)]
            down_strike = [spot - i * diff for i in range(count)]
            down_strike.pop(0)
            strikes = up_strike + down_strike
            strikes = sorted(strikes)
            self.use_strikes = strikes
        else:
            count = 10 if count is None else count
            l_st, u_st = count + 1, count
            # logger.info(f'\n{sym} {exp} df spot before numeric is \n {df["spot"]}')
            df['spot'] = pd.to_numeric(df['spot'], errors='coerce')
            # logger.info(f'\n{sym} {exp} df spot after numeric is \n {df["spot"]}')
            df = df.dropna(subset=['spot'])
            # logger.info(f'\n{sym} {exp} df spot after numeric and dropna is \n {df["spot"]}')
            if spot is not None:
                mean = spot
            else:
                mean = df['spot'].mean()
            # logger.info(f'\n{sym} {exp} mean is {mean}')
            uq_strikes = pd.to_numeric(df['strike'], errors='coerce').dropna().unique()
            # uq_strikes = df['strike'].unique()
            uq_strikes.sort()
            # strikes = uq_strikes[uq_strikes <= mean][-l_st:].tolist() + uq_strikes[uq_strikes > mean][:u_st].tolist()
            logger.info(f'\n{sym} {exp} uq strikes are \n{uq_strikes} \nand typs is {type(uq_strikes)}\n and spot is {spot} and type is {type(spot)}')
            strikes = uq_strikes[uq_strikes <= mean][-l_st:].tolist() + uq_strikes[uq_strikes > mean][:u_st].tolist()
            self.use_strikes = strikes
        # if strd_clst_nnm:
        #     logger.info(f'\nstrd_clst {sym} {exp} spot is {spot} \n use_strikes are \n{self.use_strikes}')
        # else:
        #     logger.info(f'\nconti_strd {sym} {exp} spot is {spot} \n strikes are \n{self.use_strikes}')
        logger.info(f'\n{sym} {exp} spot is {spot} \n use_strikes are \n{self.use_strikes}')
        # ----------------------------------------------------------------
        # df.to_csv(os.path.join(data_dir, f'{sym}_{exp}_before_filter.csv'), index=False)
        df_new = df[df['strike'].isin(self.use_strikes)].copy()
        # df_new.to_csv(os.path.join(data_dir, f'{sym}_{exp}_after_filter.csv'), index=False)
        # ----------------------------------------------------------------
        if strd_clst_nnm:
            df_exp = df_new.copy()

            col_to_update = ['combined_premium', 'combined_iv', 'otm_iv', 'minima']
            mask = (df_exp['call_oi'] < 10000) & (df_exp['put_oi'] < 10000)
            # mask_array = mask.to_numpy().reshape(-1, 1)
            # df_exp[['combined_premium', 'combined_iv', 'otm_iv']] = np.where(mask_array, None, df_exp[
            #     ['combined_premium', 'combined_iv', 'otm_iv']])
            for each_col in col_to_update:
                for chk_col in ['call_oi', 'put_oi']:
                    df_exp[each_col].mask((df_exp[chk_col] < 10000), None, inplace=True)
            # df_exp.to_csv(os.path.join(data_dir, f'{sym}_{exp}_before_opr.csv'), index=False)
            df_exp['change'] = df_exp['strike'].diff()
            df_exp_filtered = df_exp[df_exp['change'] <= 500]
            df_exp_filtered['counter'] = df_exp_filtered.groupby('ts')['change'].transform(
                lambda x: (x == 500).cumsum())
            df_final = df_exp_filtered[(df_exp_filtered['change'] != 500) | (df_exp_filtered['counter'] == 1)]
            df_final = df_final.drop(columns=['counter', 'call_oi', 'put_oi', 'call_iv', 'put_iv'])

            # df_final.to_csv(os.path.join(data_dir, f'{sym}_{exp}_after_opr.csv'), index=False)
            df_new = df_final.copy()

        # logger.info(f'\n{sym} {exp} df before drop is \n {df_new}')
        df_new.drop(columns=['spot', 'range'], errors='ignore', inplace=True)
        df_new.sort_values(['ts', 'strike'], inplace=True)
        # # logger.info(f'df after drop is \n {df_new}')
        if interval and len(df_new):
            valid_ts = pd.date_range(start=df_new['ts'].min(), end=df_new['ts'].max(), freq=f'{interval}min')
            # logger.info(f'Total timestamps generated: {valid_ts}')
            if len(valid_ts):
                df_new = df_new[df_new['ts'].isin(valid_ts)].copy()
        if raw:
            return df_new

        if clst:
            df_new.to_csv(os.path.join(data_dir, f'new_{sym}_{exp}_clst.csv'), index=False)
        else:
            df_new.to_csv(os.path.join(data_dir, f'new_{sym}_{exp}_strd.csv'), index=False)
        return self.df_response(df_new, to_millis=['ts'])

    @staticmethod
    def df_response(df: pd.DataFrame, to_millis: list = None) -> list[dict]:
        df = df.replace({np.NAN: None}).round(2)
        dict1 = df.to_dict('records')

        if to_millis is not None and len(to_millis) and len(df):
            for _col in to_millis:
                df[_col] = (df[_col].dt.tz_localize(IST).astype('int64') // 10 ** 9) * 1000

        dict2 = df.to_dict('records')
        return df.to_dict('records')


service = ServiceApp()
app = service.app

if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=8811, workers=5)

