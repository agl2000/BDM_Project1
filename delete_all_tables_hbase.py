
import happybase

connection = happybase.Connection('192.168.100.166',port=9090) #Change IP to yours

#s¡ha de fer a la vegada disable i delete, si a una taula li fas disable pero no delete es lia. 
for table_name in connection.tables():
    print(table_name)
    connection.disable_table(table_name)
    connection.delete_table(table_name)
    print(f"Table '{table_name}' deleted")

connection.close()