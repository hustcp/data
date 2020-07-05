import talib as ta
import numpy as np

# 计算DIF快线，用于MACD
# @param {number} short 快速EMA时间窗口
# @param {number} long 慢速EMA时间窗口
# @param {array} data 输入数据
# @param {string} field 计算字段配置


def fun_comp_dif(mydb, table_name, index):              # 输入数据库对象、表名称、索引列名称
    mydb.db_add_colume(table_name, 'DIFF', 'float(16)')  # 对数据表中增加DIFF列
    rows = mydb.db_get_rows(table_name)  # 获取数据库的总行数
    list_data = mydb.db_read_data(table_name, 'ClosePrice', 0, rows)  # 读取0-rows行的closeprice数据，以dataFrame形式存储
    closedata = list_data[0].values  # 获取数据，并转换成列表格式
    data = ta.EMA(closedata, 12)-ta.EMA(closedata, 26)      # 调用COMPMACD函数计算DIFF，并返回列表
    DIFF_data = np.nan_to_num(data)                              # 将列表中的nan转换成0，以便于float（16）格式匹配
    index_data = mydb.db_read_data(table_name, index, 0, rows)  # 读取0-rows行的索引数据，以dataFrame形式存储
    index_data = index_data.values  # 将索引数据转换成列表
    mydb.db_write_column(index_data, DIFF_data, table_name, 'DIFF')
    print("数据表 %s 已完成DIFF计算！" % table_name)
    pass


def fun_comp_dea(mydb, table_name, index):
    mydb.db_add_colume(table_name, 'DEA', 'float(16)')  # 对数据表中增加MACD列
    rows = mydb.db_get_rows(table_name)  # 获取数据库的总行数
    list_data = mydb.db_read_data(table_name, 'DIFF', 0, rows)  # 读取0-rows行的DIF数据，以dataFrame形式存储
    difdata = list_data[0].values  # 获取数据，并转换成列表格式
    data = ta.EMA(difdata, 9)      # 调用COMPMACD函数计算DEA，并返回列表
    DEA_data = np.nan_to_num(data)                    # 将列表中的nan转换成0，以便于float（16）格式匹配
    index_data = mydb.db_read_data(table_name, index, 0, rows)  # 读取0-rows行的索引数据，以dataFrame形式存储
    index_data = index_data.values  # 将索引数据转换成列表
    mydb.db_write_column(index_data, DEA_data, table_name, 'DEA')
    print("数据表 %s 已完成DEA计算！" % table_name)
    pass

def fun_comp_macd(mydb, table_name, index):
    mydb.db_add_colume(table_name, 'MACD_DIFF', 'float(16)')  # 对数据表中增加MACD列
    mydb.db_add_colume(table_name, 'MACDsignal_DEA', 'float(16)')  # 对数据表中增加MACDsignal列
    mydb.db_add_colume(table_name, 'MACDhist_DIFF_DEA', 'float(16)')  # 对数据表中增加MACDhist列
    rows = mydb.db_get_rows(table_name)    # 获取数据库的总行数
    list_data = mydb.db_read_data(table_name, 'ClosePrice', 0, rows)  # 读取0-rows行的closeprice数据，以dataFrame形式存储
    closedata = list_data[0].values  # 获取数据，并转换成列表格式
    data = ta.MACD(closedata)      # 调用COMPMACD函数计算MACD，并返回列表
    MACD_data = np.nan_to_num(data)                              # 将列表中的nan转换成0，以便于float（16）格式匹配
    index_data = mydb.db_read_data(table_name, index, 0, rows)  # 读取0-rows行的索引数据，以dataFrame形式存储
    index_data = index_data.values  # 将索引数据转换成列表
    mydb.db_write_column(index_data, MACD_data[0], table_name, 'MACD_DIFF')          # 此值为DIFF
    mydb.db_write_column(index_data, MACD_data[1], table_name, 'MACDsignal_DEA')    # 此值为DEA
    mydb.db_write_column(index_data, MACD_data[2], table_name, 'MACDhist_DIFF_DEA')      # 此值为DIFF-DEA
    print("数据表 %s 已完成MACD计算！" % table_name)
    pass


# 手算EMA12和EMA26
def fun_comp_ema(mydb, table_name, index):
    mydb.db_add_colume(table_name, 'EMA12', 'float(16)')  # 对数据表中增加MACD列
    mydb.db_add_colume(table_name, 'EMA26', 'float(16)')  # 对数据表中增加MACD列
    rows = mydb.db_get_rows(table_name)  # 获取数据库的总行数
    list_data = mydb.db_read_data(table_name, 'ClosePrice', 0, rows)  # 读取0-rows行的closeprice数据，以dataFrame形式存储
    close_data = list_data[0].values  # 获取数据，并转换成列表格式
    EMA12_data = ta.EMA(close_data,12)  # 调用COMPMACD函数计算MACD，并返回列表
    EMA26_data = ta.EMA(close_data,26)  # 调用COMPMACD函数计算MACD，并返回列表
    EMA12_data = np.nan_to_num(EMA12_data)  # 将列表中的nan转换成0，以便于float（16）格式匹配
    EMA26_data = np.nan_to_num(EMA26_data)  # 将列表中的nan转换成0，以便于float（16）格式匹配
    index_data = mydb.db_read_data(table_name, index, 0, rows)  # 读取0-rows行的索引数据，以dataFrame形式存储
    index_data = index_data.values  # 将索引数据转换成列表
    mydb.db_write_column(index_data, EMA12_data, table_name, 'EMA12')
    mydb.db_write_column(index_data, EMA26_data, table_name, 'EMA26')
    print("数据表 %s 已完成EMA12和EMA26计算！" % table_name)

# 当MACD从负数转向正数，是买的信号。
# 当MACD从正数转向负数，是卖的信号。
# 当MACD以大角度变化，表示快的移动平均线和慢的移动平均线的差距非常迅速的拉开，代表了一个市场大趋势的转变。


def fun_comp_macd1(mydb, table_name, index):
    mydb.db_add_colume(table_name, 'MACD_DIFF1', 'float(16)')  # 对数据表中增加MACD列
    mydb.db_add_colume(table_name, 'MACDsignal_DEA1', 'float(16)')  # 对数据表中增加MACDsignal列
    mydb.db_add_colume(table_name, 'MACDhist_DIFF_DEA1', 'float(16)')  # 对数据表中增加MACDhist列
    rows = mydb.db_get_rows(table_name)    # 获取数据库的总行数
    list_data = mydb.db_read_data(table_name, 'ClosePrice', 0, rows)  # 读取0-rows行的closeprice数据，以dataFrame形式存储
    closedata = list_data[0].values  # 获取数据，并转换成列表格式
    data = ta.MACD(closedata)      # 调用COMPMACD函数计算MACD，并返回列表
    MACD_data = np.nan_to_num(data)                              # 将列表中的nan转换成0，以便于float（16）格式匹配
    index_data = mydb.db_read_data(table_name, index, 0, rows)  # 读取0-rows行的索引数据，以dataFrame形式存储
    index_data = index_data.values  # 将索引数据转换成列表
    mydb.db_write_column(index_data, MACD_data[0], table_name, 'MACD_DIFF1')          # 此值为DIFF
    mydb.db_write_column(index_data, MACD_data[1], table_name, 'MACDsignal_DEA1')    # 此值为DEA
    mydb.db_write_column(index_data, MACD_data[2], table_name, 'MACDhist_DIFF_DEA1')      # 此值为DIFF-DEA
    print("数据表 %s 已完成MACD计算！" % table_name)
    pass


# 手算EMA12和EMA26
def fun_comp_ema1(mydb, table_name, index):
    mydb.db_add_colume(table_name, 'EMA121', 'float(16)')  # 对数据表中增加MACD列
    mydb.db_add_colume(table_name, 'EMA261', 'float(16)')  # 对数据表中增加MACD列
    rows = mydb.db_get_rows(table_name)  # 获取数据库的总行数
    list_data = mydb.db_read_data(table_name, 'ClosePrice', 0, rows)  # 读取0-rows行的closeprice数据，以dataFrame形式存储
    close_data = list_data[0].values  # 获取数据，并转换成列表格式
    EMA12_data = ta.EMA(close_data, 12)  # 调用COMPMACD函数计算MACD，并返回列表
    EMA26_data = ta.EMA(close_data, 26)  # 调用COMPMACD函数计算MACD，并返回列表
    EMA12_data = np.nan_to_num(EMA12_data)  # 将列表中的nan转换成0，以便于float（16）格式匹配
    EMA26_data = np.nan_to_num(EMA26_data)  # 将列表中的nan转换成0，以便于float（16）格式匹配
    index_data = mydb.db_read_data(table_name, index, 0, rows)  # 读取0-rows行的索引数据，以dataFrame形式存储
    index_data = index_data.values  # 将索引数据转换成列表
    mydb.db_write_column(index_data, EMA12_data, table_name, 'EMA121')
    mydb.db_write_column(index_data, EMA26_data, table_name, 'EMA261')
    print("数据表 %s 已完成EMA12和EMA26计算！" % table_name)
