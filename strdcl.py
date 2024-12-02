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
        req = self._straddle_response(df, raw=True, count=st_cnt, interval=30, spot = rounded_spot, diff = self.difference, sym=symbol, exp = str(expiry), strd_clst_nnm = self.strd_clst_nnm)