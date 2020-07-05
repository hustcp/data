import threading
import time
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

    # 创建数据库
    createDatabase.fun_create_database(host=host,
                                       port=port,
                                       user=user,
                                       password=password,
                                       charset=charset,
                                       databasename=databasename,
                                       tablelist=tablelist)               # 创建数据库
    print("*" * 100)
    # 插入最近的1440条数据
    for i in tablelist:
        if i == 'realtime_data_1min':
            granularity = "60"
        elif i == 'realtime_data_5min':
            granularity = "300"
        elif i == 'realtime_data_15min':
            granularity = "900"
        elif i == 'realtime_data_1h':
            granularity = "3600"
        else:
            granularity = "14400"
        insertHistory.fun_insert_history(host=host,
                                         port=port,
                                         user=user,
                                         password=password,
                                         charset=charset,
                                         databasename=databasename,
                                         table_name=str(i),
                                         instrument_id=instrument_id,
                                         granularity=granularity)
    print("*" * 100)

    # 对插入的数据进行MACD计算并增加到列中
    mydb1 = CDBMgr.Dbmgr(host=host, port=port, user=user, password=password, db=databasename)
    mydb2 = CDBMgr.Dbmgr(host=host, port=port, user=user, password=password, db=databasename)
    mydb3 = CDBMgr.Dbmgr(host=host, port=port, user=user, password=password, db=databasename)
    mydb4 = CDBMgr.Dbmgr(host=host, port=port, user=user, password=password, db=databasename)
    mydb5 = CDBMgr.Dbmgr(host=host, port=port, user=user, password=password, db=databasename)
    for i in tablelist:
        compMACD.fun_comp_macd(mydb1, i, 'time')
        compMACD.fun_comp_ema(mydb1, i, 'time')

    print("*" * 100)

    # 创建进程
    thread_getData_1min = threading.Thread(target=getRealtimeData.fun_get_data,
                                           args=(mydb1, instrument_id, "60", "realtime_data_1min"))
    thread_getData_5min = threading.Thread(target=getRealtimeData.fun_get_data,
                                           args=(mydb2, instrument_id, "300", "realtime_data_5min"))
    thread_getData_15min = threading.Thread(target=getRealtimeData.fun_get_data,
                                            args=(mydb3, instrument_id, "900", "realtime_data_15min"))
    thread_getData_1h = threading.Thread(target=getRealtimeData.fun_get_data,
                                         args=(mydb4, instrument_id, "3600", "realtime_data_1h"))
    thread_getData_4h = threading.Thread(target=getRealtimeData.fun_get_data,
                                         args=(mydb5, instrument_id, "14400", "realtime_data_4h"))

    # 启动进程
    print("数据实时获取并存档中。。。")
    thread_getData_1min.start()
    time.sleep(10)
    thread_getData_5min.start()
    time.sleep(10)
    thread_getData_15min.start()
    time.sleep(10)
    thread_getData_1h.start()
    time.sleep(10)
    thread_getData_4h.start()
'''
    # 并行计算，①对数据库数据进行处理并得到涨跌趋势
    thread_compTrend = threading.Thread(target=compTrend.fun_comp_trend())
    thread_compShape = threading.Thread(target=compShape.fun_comp_shape())           # 并行计算，②计算出k线到涨跌形态
    thread_compCrossdot = threading.Thread(target=compCrossdot.fun_comp_cross_dot())  # 并行计算，③计算穿越点
    # thread_pushMessage = threading.Thread(target=pushMessage.fun_push_message())     # 并行执行，消息推送
    thread_compTrend.start()
    thread_compShape.start()
    thread_compCrossdot.start()
'''
