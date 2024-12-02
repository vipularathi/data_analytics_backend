def _straddle_response(self, df: pd.DataFrame, raw=False, count: int = None, interval: int = None, spot: int = None, diff: int = None, sym: str=None, exp:str=None, strd_clst_nnm:bool=False):
        if diff:
            if strd_clst_nnm:
                count = 10
            else:
                count = 15
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
        if strd_clst_nnm:
            logger.info(f'\nstrd_clst {sym} {exp} spot is {spot} \n strikes are \n{self.use_strikes}')
        else:
            logger.info(f'\nconti_strd {sym} {exp} spot is {spot} \n strikes are \n{self.use_strikes}')
        df_new = df[df['strike'].isin(self.use_strikes)].copy()
        if strd_clst_nnm:
            df_exp = df_new.copy()
            # df_exp.sort_values(by=['ts','strike'])
            df_exp['change'] = df_exp['strike'].diff()
            df_exp_filtered = df_exp[df_exp['change'] <= 500]
            df_exp_filtered['counter'] = df_exp_filtered.groupby('ts')['change'].transform(
                lambda x: (x == 500).cumsum())
            df_final = df_exp_filtered[(df_exp_filtered['change'] != 500) | (df_exp_filtered['counter'] == 1)]
            df_final = df_final.drop(columns=['counter'])

            # df_final.to_csv(os.path.join(data_dir, 'nifty_nm_orig_df_strike_exp_filtered.csv'), index=False)
            # df_new.to_csv(os.path.join(data_dir, 'nifty_nm_df_strike_test.csv'), index=False)
            df_new = df_final.copy()
        logger.info(f'\n{sym} {exp} df before drop is \n {df_new}')
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
        # # logger.info(f'\nstraddle response df is \n {df_new.head()}')
        return self.df_response(df_new, to_millis=['ts'])