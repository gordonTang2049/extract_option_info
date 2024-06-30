from typing import List
import os
import pandas as pd
import numpy as np


from datetime import datetime, timedelta, date
import time
import random
import json
import pyodbc

from data import Data
from util import Util
from sql_op import Sql_op

# ======================================================================================
# ======================================================================================
# total row 10747
# , errormsg_outpath : str

server = os.environ['SERVER_NAME']
database = os.environ['DB_NAME']
username = os.environ['USER']
password = os.environ['DB_PASSWORD']
session_batch_info = os.environ['SESSION_BATCH_INO']
us_holidays = os.environ['US_HOLIDAYS']

nth_batch = os.environ['NTH_BATCH']
stoptime_range = os.environ['STOPTIME_RANGE']

sql_server = "FreeTDS"
tableName = "option"

connection_string = 'DRIVER={'+ sql_server +'};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password + ';TrustServerCertificate=yes;'

# if __name__ == "__main__":
def main():

    stoptime_From, stoptime_to = [int(t) for t in stoptime_range.split('-')]

# ========================================================
    # big batch / session batch
    session_batch_start, session_batch_end, session_batch_step = 
        [int(dataRange) for dataRange in session_batch_info.split(',')]
# ========================================================
    
# breakdown smaller batch with nth_batch
# ========================================================
    util = Util(
        nth_batch, 
        session_batch_start, 
        session_batch_end, 
        session_batch_step)

    task_batch_start, task_batch_end = util.batching()
# ========================================================

    us_holidays_str_list = [str(_date) for _date in us_holidays.split(',')]
    
    # date - year month day
    us_holidays_dates = [date(int(_date.split('-')[0]), int(_date.split('-')[1]), int(_date.split('-')[2])) for _date in us_holidays_str_list]

    print('today' , date.today())
    print('timedelta(days = 1)' , timedelta(days = 1))

    if (date.today() - timedelta(days = 1)) not in us_holidays_dates:

        cnxn = pyodbc.connect(connection_string, autocommit=True)

        df_code = pd.read_sql('SELECT TRIM([TICKER]) as TICKER, [IsShares] FROM metadata ORDER BY TICKER',cnxn).iloc[task_batch_start:task_batch_end, :2]
    
        cursor = cnxn.cursor()
    
        # STATEMENT = sql.get_insert_statement(cursor, tableName)
        
        for i in range(len(df_code)):
            
            time.sleep(random.randint(stoptime_from, stoptime_to))


            clean_ticker = str.strip(df_code.iloc[i, 0])


            print('Process :', clean_ticker)

            try:
                asset_class = "stocks"

                if df_code.iloc[i,1] == False:
                    asset_class = "etf"

                data = Data(clean_ticker, asset_class)

                df = data.process_option_table()
                
                print(df)
                
                # sql.insert_data(cursor, STATEMENT, df)

            except Exception as error:
                
                print('Error ', clean_ticker)
                print('err_msg : ', error)

            
        cnxn.commit()
        cursor.close()
        cnxn.close()

    else:
        print('=====================================================================')
        print('US Holiday')
        print('=====================================================================')


main()