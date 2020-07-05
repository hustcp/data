import threading
import numpy as np

import createDatabase
import getRealtimeData
import insertHistory
import CDBMgr
import compMACD


if __name__ == '__main__':
    host = 'www.cpaiwp.cn'
    port = 13306
    user = 'Quantitative'
    password = '123456'
    charset = 'utf8mb4'
    databasename = 'okex2'
    tablelist = ['realtime_data_1min', 'realtime_data_5min', 'realtime_data_15min',
                 'realtime_data_1h', 'realtime_data_4h']
    instrument_id = "BTC-USDT"

    # 对插入的数据进行MACD计算并增加到列中
    mydbd = CDBMgr.Dbmgr(host=host, port=port, user=user, password=password, db=databasename)
    for i in tablelist:
        compMACD.fun_comp_macd1(mydbd, i, 'time')
        compMACD.fun_comp_ema1(mydbd, i, 'time')

    print("*" * 100)