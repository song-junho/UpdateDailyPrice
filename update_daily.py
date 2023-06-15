import pandas as pd
from tqdm import tqdm
import threading

import logging
from concurrent.futures import ThreadPoolExecutor
import time
import threading



class UpdateDaily:

    def __init__(self, dict_df_stock_daily, dict_df_stock, date_start, dict_daily_deque):
        self.dict_df_stock_daily = dict_df_stock_daily
        self.dict_df_stock = dict_df_stock
        self.date_start = date_start

        self._lock = threading.Lock()
        self.dict_daily_deque = dict_daily_deque

    def update_dict_stock_daily(self, list_cmp_cd):

        date_start = self.date_start

        for cmp_cd in tqdm(list_cmp_cd):

            df_stock = self.dict_df_stock[cmp_cd].loc[date_start:]
            for v_date, rows in df_stock.iterrows():

                with self._lock:
                    self.dict_daily_deque[v_date].append(rows.to_frame().T)
                    # self.dict_df_stock_daily[v_date] = pd.concat([self.dict_df_stock_daily[v_date], rows.to_frame().T])

    def multiprocessing(self, df_krx_info):

        n = 500
        list_cmp_cd_t = sorted(df_krx_info["Symbol"])
        list_cmp_cd_t = [list_cmp_cd_t[i * n:(i + 1) * n] for i in range((len(list_cmp_cd_t) + n - 1) // n)]

        with ThreadPoolExecutor(max_workers=5) as executor:

            for list_cmp_cd in list_cmp_cd_t:
                executor.submit(self.update_dict_stock_daily, list_cmp_cd)

    def set_dict_df_stock_daily(self):

        for p_date in tqdm(self.dict_df_stock_daily.keys()):
            self.dict_df_stock_daily[p_date] = pd.concat(self.dict_daily_deque[p_date])

