
import happybase

connection = happybase.Connection('192.168.1.47',port=9090) #Change IP to yours

table_json = connection.table('csv_table_2013_distribucio_territorial_renda_familiar')

for key, data in table_json.scan(limit=5):  
    print(key, data)

connection.close()
