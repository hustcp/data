import pymysql.cursors

connection = pymysql.connect(host = '192.168.1.60',
                             port = 13306,
                             user = 'caipan',
                             password = 'wangpan0105',
                             db = 'ERP1',
                             charset = 'utf8mb4',
                             cursorclass = pymysql.cursors.DictCursor)   # 选择 Cursor 类型

try:
    with connection.cursor() as cursor:
        sql = "INSERT INTO `users` (`email`,`password`) VALUES (%s, %s)"
        cursor.execute(sql, ('webmaster@python.org', 'very-secret'))

    connection.commit()

    with connection.cursor() as cursor:
        sql = "SELECT `id`, `password` FROM `users` WHERE `email` = %s"
        cursor.execute(sql, ('webmaster@python.org',))
        result = cursor.fetchall()
        print(result)

finally:
    connection.close()