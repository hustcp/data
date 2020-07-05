# coding=utf-8
import pymysql
import t_trade_day as t_trade_day
import t_kdata as t_kdata


class CDBMgr:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connect = None
        self.trade_day = None
        self.k_data = None
        # self.connect_db()

    # connect database
    def connect_db(self):
        self.connect = pymysql.connect(self.host, self.port, self.user, self.password, self.database, charset="utf8")
        self.trade_day = t_trade_day.CT_Tradeday(self.connect)
        self.k_data = t_kdata.CT_Kdata(self.connect)

    # 增加k线数据
    def add_kdata(self, code, trade_day, open, close, high, low, vol):
        if self.connect is None:
            return -1

        self.k_data.add_kdata(code, trade_day, open, close, high, low, vol)

    # 根据股票代码，删除表中的数据
    def del_kdata(self, code):
        if self.connect is None:
            return -1

        self.k_data.del_kdata(code)

    def modify_kdata(self, code, open, close, high, low, vol):
        if self.connect is None:
            return -1

        self.k_data.modify_kdata(code, open, close, high, low, vol)

    # 判断某个股票代码是否存在
    def exist_kdata(self, code):
        if self.connect is None:
            return -1

        return self.k_data.exist_kdata(code)

    def query_kdata(self, code):
        if self.connect is None:
            return -1

        return self.k_data.query_kdata(code)

    ########################################################################
    # 判断某个交易日期是否存在
    def exist_trade_day(self, trade_day):
        if self.connect is None:
            return -1

        return self.trade_day.exist_trade_day(trade_day)

    def add_trade_day(self, trade_day):
        if self.connect is None:
            return -1

        return self.trade_day.add_tradeday(trade_day)