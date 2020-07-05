import requests
import datetime
import time
from fake_useragent import UserAgent

import pushMessage


def fun_get_instrument():       # 获取币对，存入dict
    url_instrument = "https://www.okex.me/api/spot/v3/instruments"
    r = requests.get(url_instrument)
    response_instrument_dict = r.json()
    return response_instrument_dict         # 此函数返回所有的币币对，但在此案例中仅使用BTC-USDT


# 函数fun_get_data获取指定币对的实时价格数据，此处使用BTC-USDT币对数据
# 获取的数据存入列表list，其长度可能为0,1,2，再存入数据库，返回为空
# def fun_get_data(instrument_id="BTC-USDT", start="", end="", granularity="60"):
def fun_get_data(database_obj, instrument_id="BTC-USDT", granularity="60", table_name="realtime_data_1min"):
    # 获取需查询的时间段长度值
    delta_time = int(int(granularity)/60)
    data_list = []
    n = 0               # 数据请求失败的次数，失败后，下一次请求的数据时间段应相应延长n倍
    headers = UserAgent().random
    # 进入循环查询过程
    while True:
        # 计算查询时间段起始值
        now_time = datetime.datetime.utcnow()
        start = (now_time + datetime.timedelta(minutes=-delta_time*(n+1))).replace(microsecond=0).isoformat()
        end = now_time.replace(microsecond=0).isoformat()
        # 拼接请求链接
        url_real_data = "https://www.okex.me/api/spot/v3/instruments/" + str(instrument_id) + "/candles?granularity=" \
                        + str(granularity) + "&start=" + str(start) + ".000Z&end=" + str(end) + ".000Z"
        # print(url_real_data)
        try:
            s = requests.session()
            s.keep_alive = False
            s.adapters.DEFAULT_RETRIES = 100
            r = s.get(url_real_data, headers={"User-Agent": headers}, timeout=12)     # 获得请求数据
        except requests.exceptions.ConnectionError as e:
            print('error', e.args)
            pushMessage.fun_push_message("the error is " + str(e.args))
            n += 1
        else:
            n = 0
            temp = r.json()     # 将请求的数据解码后存入列表
            temp.reverse()
            data_list.extend(temp)        # 将请求的数据解码后存入列表
            if len(data_list) == 0:     # 对获取的数据为空列表的处理方式为直接结束函数，重新查询
                continue
            print(data_list, "\tdelta_time = ", delta_time)      # 打印获取的数据及当前未存入数据库的数据（用于调试）

            # 将获取到的列表数据存入数据库[[time, open, high, low, close, volume],[],...]
            for i in data_list:
                # 修改
                sql = "SELECT MACDsignal_DEA,EMA12,EMA26 FROM " + str(table_name) + " order by time DESC limit 1"
                history_data = database_obj.db_select_sql(sql)  # 返回dataFrame类型数据
                EMA12 = 2 / 13 * float(i[4]) + 11 / 13 * float(history_data.at[0, 1])   # 实时计算MACD
                EMA26 = 2 / 27 * float(i[4]) + 25 / 27 * float(history_data.at[0, 2])
                MACD_DIFF = EMA12 - EMA26
                MACDsignal_DEA = 2 / 10 * MACD_DIFF + 8 / 10 * float(history_data.at[0, 0])

                MACDhist_DIFF_DEA = MACD_DIFF - MACDsignal_DEA
                sql = "REPLACE INTO " + str(table_name) \
                      + " (time, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, " \
                        "MACD_DIFF, MACDsignal_DEA, MACDhist_DIFF_DEA, EMA12, EMA26) VALUES (\"" \
                      + str(i[0]) + "\"," \
                      + str(i[1]) + "," \
                      + str(i[2]) + "," \
                      + str(i[3]) + "," \
                      + str(i[4]) + "," \
                      + str(i[5]) + "," \
                      + str(MACD_DIFF) + "," \
                      + str(MACDsignal_DEA) + "," \
                      + str(MACDhist_DIFF_DEA) + "," \
                      + str(EMA12) + "," \
                      + str(EMA26) + ")"  # 编写sql语句
                # print(sql)
                database_obj.db_exe_sql(sql)  # 执行sql语句
            data_list = []

                # 修改
                # sql = "REPLACE INTO " + str(table_name) + \
                #        " (time, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume) VALUES (\"" \
                #        + str(i[0]) + "\"," + str(i[1]) + "," + str(i[2]) \
                #        + "," + str(i[3]) + "," + str(i[4]) + "," + str(i[5]) + ")"      # 编写sql语句
                # database_obj.db_exe_sql(sql)             # 执行sql语句

        time.sleep(int(granularity))
