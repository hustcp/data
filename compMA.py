#
# K线数据
# 开盘价，收盘价，最低价，最高价
# @param {number} day_count MA时间窗口
# @param {array} data 输入数据
# @param {number} field 收盘价的位置


def fun_comp_ma(day_count, data, field):
    ma = []
    for i in range(len(data)):
        if i < day_count-1:
            ma.append('0')
            continue
        sum = 0
        for j in range(day_count):
            sum += data[field]
            ma.append(sum / day_count)
    return ma
    pass
