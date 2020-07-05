import pymysql


def fun_save_data(data_list, table_name):   # 将列表数据存入数据库[[time, open, high, low, close, volume],[],...]
    if len(data_list) == 0:                 # 对输入为空列表的处理直接结束
        return
    connection = pymysql.connect(host='www.cpaiwp.cn',
                                 port=13306,
                                 user='caipan',
                                 password='wangpan0105',
                                 db='okex',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            if len(data_list) == 2:         # 用于对一次查到两个数据的情况进行处理
                sql1 = "INSERT INTO" + str(table_name) + "(" + str(data_list[1][0]) + str(data_list[1][1]) + \
                       str(data_list[1][2]) + str(data_list[1][3]) + str(data_list[1][4]) + ")"  # 数据插入数据表
                cursor.execute(sql1)
            sql2 = "INSERT INTO" + str(table_name) + "(" + str(data_list[0][0]) + str(data_list[0][1]) + \
                   str(data_list[0][2]) + str(data_list[0][3]) + str(data_list[1][4]) + ")"  # 数据插入数据表
            cursor.execute(sql2)
        connection.commit()

    finally:
        connection.close()
    pass
