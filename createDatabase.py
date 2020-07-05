import pymysql.cursors


def fun_create_database(host, port, user, password, charset, databasename, tablelist):
    connection = pymysql.connect(host=host,
                                 port=port,
                                 user=user,
                                 password=password,
                                 charset=charset,
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            sql1 = "CREATE DATABASE IF NOT EXISTS " + str(databasename)   # 创建数据库
            print("数据库 %s 创建中。。。" % databasename)
            sql2 = "use " + str(databasename)      # 选择数据库
            cursor.execute(sql1)
            cursor.execute(sql2)
            for i in tablelist:
                sql = "CREATE TABLE IF NOT EXISTS " + str(i) + " ("\
                      "time        char(30)    NOT NULL, " \
                      "OpenPrice   float(16)     NULL, " \
                      "HighPrice   float(16)     NULL, " \
                      "LowPrice    float(16)     NULL, " \
                      "ClosePrice  float(16)     NULL, " \
                      "Volume      float(20)    NULL, " \
                      "PRIMARY KEY (time))"                 # 创建数据表
                cursor.execute(sql)
                print("数据表 %s 创建中。。。" % i)
        connection.commit()
        print("数据库及表格已完成创建！")
    finally:
        connection.close()
    pass
