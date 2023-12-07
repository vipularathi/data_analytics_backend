import json
import traceback
from datetime import datetime
from time import sleep

import numpy as np
import pandas as pd
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from colorama import Style, Fore
from dateutil.relativedelta import relativedelta

from common import logger, today
from contracts import get_req_contracts
from db_ops import DBHandler
from greeks import get_greeks_intraday


class SnapAnalysis:

    def __init__(self, ins_df, tokens, token_xref, shared_xref):
        super().__init__()
        self.ins_df = ins_df
        self.tokens = tokens
        self.token_xref = token_xref
        self.shared_xref = shared_xref

        self.opt_df = None
        self.prepare_meta()

        self.scheduler = None
        self.scheduler: BackgroundScheduler = self.init_scheduler()
        self.scheduler.start()  # Scheduler starts

    def prepare_meta(self):
        req_df, token, token_xref = get_req_contracts()
        meta_df = req_df[req_df['segment'] == 'NFO-OPT'].copy()
        renames = {'tradingsymbol': 'symbol', 'name': 'underlying', 'instrument_type': 'opt'}
        opt_df = meta_df[['tradingsymbol', 'name', 'expiry', 'strike', 'instrument_type']].copy().rename(columns=renames)
        opt_df['expiry'] = opt_df['expiry'].apply(lambda x: x + relativedelta(hour=15, minute=30))
        self.opt_df = opt_df

    def init_scheduler(self):
        if self.scheduler is not None:
            return self.scheduler

        executors = {
            'default': {'type': 'threadpool', 'max_workers': 5}
        }

        job_defaults = {
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 10
        }
        self.scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults, timezone='Asia/Kolkata',
                                             logger=logger)
        self.add_jobs_to_scheduler()
        return self.scheduler

    def add_jobs_to_scheduler(self):
        mkt_open = today + relativedelta(hour=9, minute=15)
        mkt_close = today + relativedelta(hour=15, minute=30)

        # Add jobs
        trigger_pre = IntervalTrigger(minutes=1, start_date=mkt_open - relativedelta(minutes=5), end_date=mkt_open)
        trigger_snap = IntervalTrigger(minutes=1, start_date=mkt_open, end_date=mkt_close)
        self.scheduler.add_job(self.run_analysis, trigger_pre, kwargs=dict(calc=False), id='snap_1',
                               name=f'SnapDataAnalysisPre')
        self.scheduler.add_job(self.run_analysis, trigger_snap, kwargs=dict(calc=True), id='snap_2',
                               name=f'SnapDataAnalysis')

    def run_analysis(self, calc=True):
        dt = datetime.now(tz=pytz.timezone('Asia/Kolkata')).replace(microsecond=0)
        xref = self.shared_xref.copy()
        logger.info(list(xref.keys()))
        data = {'timestamp': dt.isoformat(), 'snap': xref}
        db_data = json.loads(json.dumps(data, default=str))
        DBHandler.insert_snap_data([db_data])

        if calc:
            logger.info(f'calc values for {dt}')
            greeks_df = self.opt_calc(snap=xref, dt=dt)
            self.straddle_calc(greeks_df)

    def opt_calc(self, snap, dt):
        opt_df = self.opt_df.copy()
        opt_df['ltp'] = opt_df['symbol'].apply(self.get_ltp, args=(snap,))
        opt_df['oi'] = opt_df['symbol'].apply(self.get_oi, args=(snap,))
        opt_df['spot'] = opt_df['underlying'].apply(self.get_ltp, args=(snap,))
        greeks = opt_df.apply(
            lambda x: pd.Series(get_greeks_intraday(x['spot'], x['strike'], x['expiry'], x['opt'], x['ltp'], dt)),
            axis=1)
        df = opt_df.join(greeks)
        df.insert(0, 'timestamp', dt)
        DBHandler.insert_opt_greeks(df.replace({np.NAN: None}).to_dict('records'))
        return df

    @staticmethod
    def straddle_calc(df: pd.DataFrame):
        call_df = df[(df['opt'] == 'CE')].copy()
        put_df = df[(df['opt'] == 'PE')].copy()

        # straddle values
        oc_df = call_df.merge(put_df, on=['timestamp', 'underlying', 'expiry', 'strike'], how='outer', suffixes=('_c', '_p'))
        oc_df.sort_values(['timestamp', 'underlying', 'expiry', 'strike'], inplace=True)
        non_zero = (oc_df['ltp_c'] != 0) & (oc_df['ltp_p'] != 0)
        oc_df.loc[non_zero, 'combined_premium'] = (oc_df['ltp_c'] + oc_df['ltp_p'])[non_zero]
        oc_df.loc[non_zero, 'combined_iv'] = (oc_df[['iv_c', 'iv_p']].mean(axis=1))[non_zero]
        min_combined = oc_df.groupby(['timestamp', 'underlying', 'expiry'], as_index=False).agg({'combined_premium': 'min'})
        min_combined['minima'] = True
        minima_df = oc_df.merge(min_combined, on=['timestamp', 'underlying', 'expiry', 'combined_premium'], how='left')
        # oc_df['minima'] = oc_df['combined_premium'] == min_combined

        req_cols = ['timestamp', 'underlying', 'expiry', 'strike', 'symbol_c', 'symbol_p', 'spot_c', 'ltp_c', 'ltp_p',
                    'oi_c', 'oi_p', 'iv_c', 'iv_p', 'combined_premium', 'combined_iv', 'minima']
        req_straddle_df = minima_df[req_cols].copy()
        cols_renames = {'symbol_c': 'call', 'symbol_p': 'put', 'spot_c': 'spot', 'ltp_c': 'call_price',
                        'ltp_p': 'put_price', 'oi_c': 'call_oi', 'oi_p': 'put_oi', 'iv_c': 'call_iv', 'iv_p': 'put_iv'}
        req_straddle_df.rename(columns=cols_renames, inplace=True)
        # insert req_straddle_df
        DBHandler.insert_opt_straddle(req_straddle_df.replace({np.NAN: None}).to_dict('records'))
        return req_straddle_df

    @staticmethod
    def get_ltp(symbol: str, xref: dict):
        return xref.get(symbol, {}).get('last_price', None)

    @staticmethod
    def get_oi(symbol: str, xref: dict):
        return xref.get(symbol, {}).get('oi', None)


def start_analysis(ins_df, tokens, token_xref, shared_xref):
    try:
        SnapAnalysis(ins_df, tokens, token_xref, shared_xref)

        while True:
            sleep(10)
    except Exception as exc:
        logger.error(f'{Style.BRIGHT}{Fore.GREEN}Error in initiating Strategy Data Processor: {exc}\n{"*" * 15} Requires immediate attention {"*" * 15}{Style.RESET_ALL}')
        logger.error(traceback.format_exc())
