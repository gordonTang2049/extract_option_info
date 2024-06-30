import subprocess
import json
import pandas as pd 
from pandas import json_normalize
import numpy as np
import re
from datetime import datetime, timedelta, date
import os

class Data:
    def __init__(
        self, 
        ticker : str,
        asset_class : str):

        self.ticker = ticker
        self.asset_class = asset_class
    
    def fetch_option_data(self) -> pd.DataFrame:
        js = subprocess.run([
            'node', 
            '/opt/app/index.js', 
            self.ticker, 
            self.asset_class],
            capture_output=True,
            text=True, 
            check=True)

        data = json.loads(js.stdout)
        
        print(data)

        df = pd.json_normalize(data['table']['rows'], max_level=0)

        return df

    def process_option_table(self) -> pd.DataFrame:
        df = self.fetch_option_data()
        
        print(df)

        df['UNDERLYING_TRADE_PRICE'] = float(re.search(r"\$\d+\.\d+\s", data['lastTrade']).group().replace("$","").strip())
        
        # =========================================================================
        # =========================================================================
        # needed to be yesterday
        yesterday = date.today() - timedelta(days = 1)
        df['TRADE_DATE'] = yesterday.strftime("%Y-%m-%d")

        # =========================================================================
        # =========================================================================

        df['asset_class'] = self.asset_class

        df.expirygroup = df.expirygroup.replace("",np.nan)
        df.expirygroup = df.expirygroup.ffill()
        df.expirygroup = pd.to_datetime(df.expirygroup, format="%B %d, %Y")
        df = df.replace("--",0)
        df = df[~df.expiryDate.isnull()]
        df[['TICKER', 'OPTION_ID']] = df.drillDownURL.str.rsplit("/",n=1).apply(lambda x : pd.Series([x[1].split('-')[0], x[1].split('-')[-1]]))
        
        df.drop(columns=['expiryDate', 'c_colour', 'p_colour','drillDownURL'], inplace=True)

        convert_dict =  {
        'c_Last': float,
        'c_Change': float,
        'c_Bid': float,
        'c_Ask' : float,
        'c_Volume' : int,
        'c_Openinterest' : int,
        'strike': float,
        'p_Last': float,
        'p_Change': float,
        'p_Bid': float,
        'p_Ask': float,
        'p_Volume': int,
        'p_Openinterest': int}

        df = df.astype(convert_dict)

        df = df[[
        'TRADE_DATE', 
        'expirygroup',
        'OPTION_ID',
        'TICKER',
        'asset_class',
        'UNDERLYING_TRADE_PRICE',
        'c_Last',
        'c_Change',
        'c_Bid',
        'c_Ask',
        'c_Volume',
        'c_Openinterest',
        'strike',
        'p_Last',
        'p_Change',
        'p_Bid',
        'p_Ask',
        'p_Volume',
        'p_Openinterest']]

        return df