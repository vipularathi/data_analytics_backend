from datetime import date, datetime, time

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from common import IST, yesterday, today
from contracts import get_req_contracts
from db_ops import DBHandler


class ServiceApp:

    def __init__(self):
        super().__init__()
        self.app = FastAPI(title='ARathi', description='ARathi', docs_url='/docs', openapi_url='/openapi.json')
        self.app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"],
                                allow_headers=["*"])

        self.add_routes()
        self.symbol_expiry_map = None
        self.use_otm_iv = True

    def add_routes(self):
        self.app.add_api_route('/', methods=['GET'], endpoint=self.default)
        self.app.add_api_route('/symbol', methods=['GET'], endpoint=self.get_symbols)
        self.app.add_api_route('/straddle/minima', methods=['GET'], endpoint=self.fetch_straddle_minima)
        self.app.add_api_route('/straddle/iv', methods=['GET'], endpoint=self.fetch_straddle_iv)
        self.app.add_api_route('/straddle/cluster', methods=['GET'], endpoint=self.fetch_straddle_cluster)

    @staticmethod
    def default():
        body = {'status': 'success', 'message': '', 'data': [], 'response_code': None}
        return JSONResponse(content=jsonable_encoder(body), status_code=status.HTTP_200_OK)

    def get_symbols(self):
        if self.symbol_expiry_map is None:
            ins_df, tokens, token_xref = get_req_contracts()
            ins_df['expiry'] = ins_df['expiry'].dt.strftime('%Y-%m-%d')
            agg = ins_df[ins_df['instrument_type'].isin(['CE', 'PE'])].groupby(['name'], as_index=False).agg({'expiry': set, 'tradingsymbol': 'count'})
            agg['expiry'] = agg['expiry'].apply(lambda x: sorted(list(x)))
            self.symbol_expiry_map = agg.to_dict('records')
        return self.symbol_expiry_map

    def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None), interval: int = Query(5), cont: bool = Query(False)):
        if cont:
            df = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
            df['prev'] = df['ts'] < today
        else:
            df = DBHandler.get_straddle_minima(symbol, expiry)
            df['prev'] = False
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        return self._straddle_response(df, count=st_cnt, interval=interval)

    def fetch_straddle_iv(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None), interval: int = Query(5)):
        df = DBHandler.get_straddle_iv_data(symbol, expiry)
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        return self._straddle_response(df, count=st_cnt, interval=interval)

    def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=15), interval: int = Query(5)):
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
        req1 = self._straddle_response(df, raw=True, count=st_cnt, interval=15)
        req1 = req1[req1['ts'].dt.time <= break_ts].copy()
        req2 = self._straddle_response(df, raw=True, count=st_cnt, interval=30)
        req2 = req2[req2['ts'].dt.time > break_ts].copy()  # prev day covered here
        d = [req1, req2]
        d = [_d for _d in d if len(d)]
        if d:
            req = pd.concat(d, ignore_index=True, sort=False)
            req.sort_values(['ts', 'strike'], inplace=True)
        else:
            req = pd.DataFrame(columns=req1.columns)
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
        return self.df_response(df, to_millis=['ts'])

    @staticmethod
    def df_response(df: pd.DataFrame, to_millis: list = None) -> list[dict]:
        df = df.replace({np.NAN: None}).round(2)
        if to_millis is not None and len(to_millis) and len(df):
            for _col in to_millis:
                df[_col] = (df[_col].dt.tz_localize(IST).astype('int64') // 10**9) * 1000
        return df.to_dict('records')


service = ServiceApp()
app = service.app


if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=8501, workers=2)
