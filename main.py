import pickle
import FinanceDataReader as fdr
from tqdm import tqdm
import exchange_calendars as ecals
import pandas as pd
from datetime import datetime
from update_daily import UpdateDaily


if __name__ == "__main__":

    with open(r"D:\MyProject\StockPrice\DictDfStock.pickle", 'rb') as fr:
        dict_df_stock = pickle.load(fr)

    with open(r"D:\MyProject\StockPrice\DictDfStockDaily.pickle", 'rb') as fr:
        dict_df_stock_daily = pickle.load(fr)

    # krx 데이터 호출
    df_krx_info = fdr.StockListing("KRX")
    df_krx_info = df_krx_info[df_krx_info["Market"].isin(["KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"])]
    df_krx_info = df_krx_info[~df_krx_info["Name"].str.contains("스팩")]
    df_krx_info = df_krx_info.sort_values("Code").reset_index(drop=True)
    df_krx_info = df_krx_info.rename(columns={"Code": "Symbol"})
    df_krx_info = df_krx_info[~(df_krx_info["Symbol"].str[-1] != "0")].reset_index(drop=True)

    XKRX = ecals.get_calendar("XKRX")

    is_update_all = -1
    if is_update_all == -1:
        # 전체 일자 업데이트
        dict_df_stock_daily = {}
        for v_date in tqdm(XKRX.schedule.index):
            if v_date > datetime.today():
                break
            dict_df_stock_daily[v_date] = pd.DataFrame()

        date_start = min(dict_df_stock_daily.keys())
    else:
        # 최신 업데이트 일자 이후 업데이트
        date_start = min(dict_df_stock_daily.keys())


    update_price = UpdateDaily(dict_df_stock_daily, dict_df_stock, date_start)
    update_price.multiprocessing(df_krx_info)

    dict_df_stock_daily = update_price.dict_df_stock_daily

    # save
    with open(r"D:\MyProject\StockPrice\DictDfStockDaily.pickle", 'wb') as fw:
        pickle.dump(dict_df_stock_daily, fw)