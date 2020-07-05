import requests
import datetime
import pymysql


def fun_insert_history(host, port, user, password, charset, databasename, table_name, instrument_id="BTC-USDT", granularity="60"):
    # 获取需查询的时间段
    data_list = []
    delta_time_min = int(int(granularity)/60)
    now_time = datetime.datetime.utcnow()
    endtime = now_time
    starttime =now_time+ datetime.timedelta(minutes=-(delta_time_min * 200))
    for i in range(8):       # 历史数据查询8次，每次200条，可覆盖1440条
        end = endtime.replace(microsecond=0).isoformat()
        start = starttime.replace(microsecond=0).isoformat()
        url_real_data = "https://www.okex.me/api/spot/v3/instruments/" + str(instrument_id) + "/candles?granularity=" \
              + str(granularity) + "&start=" + str(start) + ".000Z&end=" + str(end) + ".000Z"
        r = requests.get(url_real_data)
        data_list.extend(r.json())        # 将请求的数据存入列表
        endtime = starttime
        starttime = starttime+ datetime.timedelta(minutes=-(delta_time_min * 200))

    try:
        connection = pymysql.connect(host=host,
                                     port=port,
                                     user=user,
                                     password=password,
                                     db=databasename,
                                     charset=charset,
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                for i in data_list:
                    sql1 = "REPLACE INTO " + str(table_name) + \
                           " (time, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume) VALUES (\"" + str(i[0]) + "\"," \
                           + str(i[1]) + "," + str(i[2]) + "," + str(i[3]) + "," + str(i[4]) + "," + str(i[5]) + ")"
                    cursor.execute(sql1)
            connection.commit()
            print("数据表 %s 已完成历史数据导入！" % table_name)
        except:
            print("数据写入失败！")
        finally:
            connection.close()
    except pymysql.err.OperationalError:
        print("数据库连接超时！")
    except:
        print("数据库连接失败！")


