import happybase
from pymongo import MongoClient



def list_mongodb_collections(host, port):
    # Connect to MongoDB
    client = MongoClient(host, port)

    # List available collections (tables)
    collections = client.list_database_names()
    print("Available collections (tables):", collections)

    # Close the connection
    client.close()



def create_hbase_table(host, table_name, column_families):
    # Connect to HBase
    connection = happybase.Connection(host, port=9090)
    connection.open()

    # Check if the table already exists
    # if table_name.encode() in connection.tables():
    #     print(f"Table '{table_name}' already exists.")
    #     return

    # Create the table
    connection.create_table(table_name, column_families)

    # Close the connection
    connection.close()

    print(f"Table '{table_name}' created successfully.")




def main():
    #Define all the MongoDB variables to connect
    # MongoDB host and port
    mongodb_host = 'localhost'
    mongodb_port = 27017

    #Define all the Hbase variables to connect
    # HBase host
    hbase_host = '192.168.100.166'  # Replace with your HBase host IP address


    #Ask the user what action does he want to perform

    action_text= "Select what action you want to perform: \n 1. Add new data source \n 2. Consult existing tables \nEnter your choice: "

    action=input(action_text)

    if action=='1':
        action_text= "\nSelect the origin of the new data source you want to add:\n 1. REST API\n 2. File System\nEnter your choice: "
        action=input(action_text)
        if action=='1':
            print("Afegir des d'una API")
        elif action=='2':
            print("Afegir des d'un File System")
            
        else: print("Wrong input")

    elif action == '2':
        print("Consultar taules existents")
    
    else: print("Wrong input")

 

    # List collections
    list_mongodb_collections(mongodb_host, mongodb_port)







    

    # # Table name
    # table_name = 'your_table_name'

    # # Column families (specify as a dictionary where keys are column family names and values are configuration options)
    # column_families = {
    #     'cf1': {},  # Change 'cf1' to the desired column family name
    #     'cf2': {}   # Add more column families as needed
    # }

    # # Create the table
    # create_hbase_table(hbase_host, table_name, column_families)





    # # Connect to HBase
    # connection = happybase.Connection('192.168.100.166', port=2181)  # Assuming default HBase Thrift server port is 9090
    # connection.open()

    # # List available tables
    # tables = connection.tables()
    # print("Available tables:", tables)

    # # Choose a table to scan
    # table_name = 'your_table_name_here'

    # # Scan the table and print the rows
    # table = connection.table(table_name)
    # print(f"Scanning table '{table_name}':")
    # for key, data in table.scan():
    #     print(f"Row key: {key}, Data: {data}")

    # # Close the connection
    # connection.close()

if __name__ == "__main__":
    main()