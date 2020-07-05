import sqlite3
conn = sqlite3.connect(r'C:\Users\Administrator\Desktop\iris\database.sqlite')

c = conn.cursor()
c.row_factory = sqlite3.Row
c.execute('select * from Iris')
results = c.fetchone()
print(results['PetalLengthCm'])