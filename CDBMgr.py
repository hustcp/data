import pymysql
import time
import pandas as pd

import pushMessage


# 建立数据库管理类
class Dbmgr():
    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.charset = 'utf8mb4'
        # self.cursorclass = pymysql.cursors.DictCursor
        self.cursorclass = pymysql.cursors.Cursor
        self.db_connect = None
        self._db_connect()

    # 尝试连接数据库
    def _db_connect(self):
        try:
            self.db_connect = pymysql.connect(host=self.host,
                                              port=self.port,
                                              user=self.user,
                                              password=self.password,
                                              db=self.db,
                                              charset=self.charset,
                                              cursorclass=self.cursorclass)
            return True
        except pymysql.err.OperationalError:
            print("类Dbmgr-函数db_connect中数据库连接超时！！！")
            pushMessage.fun_push_message("类Dbmgr-函数db_connect中数据库连接超时！！！")
            return False
        except pymysql.err.InterfaceError:
            print("类Dbmgr-函数db_connect中异常原因导致数据库连接失败！请及时检查连接！！！")
            pushMessage.fun_push_message("类Dbmgr-函数db_connect中异常原因导致数据库连接失败！请及时检查连接！！！")
            return False

    def _db_reconnect(self, num=28800, stime=3):        # 重试连接总次数为1天,这里根据实际情况自己设置,如果服务器宕机1天都没发现就报错
        _number = 0
        _status = True                  # _status 为True表示需要重连，为False表示不需要重连
        while _status and _number <= num:
            try:
                self.db_connect.ping()     # Ping成功则_status置为False，否则仍为True且进行except操作
                _status = False
            except:
                if self._db_connect():  # 进行重连，如果重连成功，则退出，_status置为False，否则每隔stime重连一次。
                    _status = False
                    break
                _number += 1
                time.sleep(stime)

    def db_write_data(self, data_list, table_name):     # 将data_list中数据写入数据库
        try:
            self._db_reconnect()
            if data_list is None:
                return True
            with self.db_connect.cursor() as cursor:
                for i in data_list:
                    if type(i) == list:
                        sql1 = "REPLACE INTO " + str(table_name) + \
                               " (time, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume) VALUES (\"" \
                               + str(i[0]) + "\"," + str(i[1]) + "," + str(i[2]) \
                               + "," + str(i[3]) + "," + str(i[4]) + "," + str(i[5]) + ")"
                    elif type(i) == dict:
                        sql1 = "REPLACE INTO " + str(table_name) + \
                               " (time, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume) VALUES (\"" \
                               + str(i['time']) + "\"," + str(i['OpenPrice']) + "," + str(i['HighPrice']) \
                               + "," + str(i['LowPrice']) + "," + str(i['ClosePrice']) + "," + str(i['Volume']) + ")"
                    else:
                        print("类Dbmgr-函数db_write_data中无法识别需要写入的数据，请检查数据输入！！！")
                        pushMessage.fun_push_message("类Dbmgr-函数db_write_data中无法识别需要写入的数据，请检查数据输入！！！")
                        return False
                    cursor.execute(sql1)
                self.db_connect.commit()
            # self.db_connect.close()
        except Exception:
            self.db_connect.rollback()
            print("类Dbmgr-函数db_write_data中数据写入失败,请及时检查异常！！")
            pushMessage.fun_push_message("类Dbmgr-函数db_write_data中数据写入失败,请及时检查异常！！")
            return False

    # 对数据库某一列进行写入操作
    def db_write_column(self, index_list, data_list, table_name, column_name):  # 将data_list中数据写入数据库
        try:
            self._db_reconnect()
            if data_list is None:
                return True
            with self.db_connect.cursor() as cursor:
                for i in range(len(data_list)):
                    sql2 = "UPDATE " + str(table_name) + " SET " + str(column_name) + "=" + str(data_list[i]) + \
                           " WHERE " + "time=\"" + str(index_list[i][0]) + "\""
                    # print(sql2)
                    cursor.execute(sql2)
                self.db_connect.commit()
        except Exception:
            self.db_connect.rollback()
            print("类Dbmgr-函数db_write_column中数据写入失败,请及时检查异常！！")
            pushMessage.fun_push_message("类Dbmgr-函数db_write_column中数据写入失败,请及时检查异常！！")
            return False

    # 对数据库进行数据读取，返回dataFrame类型的结果
    def db_read_data(self, table_name, key, m, n):         # 读取数据第m-n行的"column_name"列数据，若为多列，则用','隔开
        try:
            self._db_reconnect()
            # print("read-0")
            with self.db_connect.cursor() as cursor:
                sql3 = "SELECT " + str(key) + " FROM " + str(table_name) + " limit " + str(m) + "," + str(n)
                # print(sql3)
                cursor.execute(sql3)
                self.db_connect.commit()
                result_data = cursor.fetchall()
                # print("1db_read_data" + ("*" * 80))
                # print(result_data)
                # print(type(result_data))
                # print("2db_read_data" + ("*" * 80))
                df = pd.DataFrame(list(result_data))
                return df      # 返回dataFrame类型的结果
        except Exception:
            self.db_connect.rollback()
            print("类Dbmgr-函数db_read_data中数据读取失败,请及时检查异常！！")
            pushMessage.fun_push_message("类Dbmgr-函数db_read_data中数据读取失败,请及时检查异常！！")
            return False

    def db_get_rows(self, table_name):
        try:
            self._db_reconnect()
            with self.db_connect.cursor() as cursor:
                sql4 = "SELECT COUNT(*) FROM " + str(table_name)
                cursor.execute(sql4)
                result_rows = cursor.fetchall()
                return result_rows[0][0]
        except Exception:
            self.db_connect.rollback()
            return False

    def db_add_colume(self, table_name, new_column, new_column_type):
        try:
            self._db_reconnect()
            with self.db_connect.cursor() as cursor:
                sql5 = "ALTER TABLE " + str(table_name) + " ADD COLUMN " + str(new_column) + " " + str(new_column_type)
                # print(sql5)
                cursor.execute(sql5)
            self.db_connect.commit()
            return True
        except Exception:
            self.db_connect.rollback()
            return False

    def db_exe_sql(self, sql6):
        try:
            self._db_reconnect()
            with self.db_connect.cursor() as cursor:
                cursor.execute(sql6)
            self.db_connect.commit()
            return True
        except Exception:
            self.db_connect.rollback()
            return False

    def db_select_sql(self, sql7):
        try:
            self._db_reconnect()
            with self.db_connect.cursor() as cursor:
                cursor.execute(sql7)
            self.db_connect.commit()
            result_data = cursor.fetchall()
            df = pd.DataFrame(list(result_data))
            return df
        except Exception:
            self.db_connect.rollback()
            return False

    def db_close(self):
        self.db_connect.ping()
        self.db_connect.close()

