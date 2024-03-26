import data_collectors as dc
import data_persistence_loader as dpl

import happybase
from pymongo import MongoClient



def get_mongodb_collections(host, port):
    # Connect to MongoDB
    client = MongoClient(host, port)

    # List available collections (tables)
    # Access the database
    db = client['Persistent_Landing']

    # List all collections in the database
    collections = db.list_collection_names()

    # Print the list of collections
    # print("Collections in the 'Persistent_Landing' database:\n", collections)


    # Close the connection
    client.close()

    return(collections)


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

    # List collections MongoDB
    mongodb_collections=get_mongodb_collections(mongodb_host, mongodb_port)
    print(mongodb_collections)

    #Define all the Hbase variables to connect
    # HBase host
    hbase_host = '192.168.1.112'  # Replace with your HBase host IP address


    #Ask the user what action does he want to perform

    action_text= "Select what action you want to perform: \n 1. Add new data \n 2. Consult existing tables \nEnter your choice: "

    action=input(action_text)

    if action == '1':
        action_text = "\nSelect the source of the new data you want to add:\n 1. REST API\n 2. File System\nEnter your choice: "
        action = input(action_text)

        if action == '1':
            print("Adding data from a REST API")
            
        elif action == '2':
            action_text = "\nInsert the folder URL: "
            folder_url = input(action_text)

            print("\nSelect the name of the dataset from the list in MongoDB:")

            i=1
            for collection in mongodb_collections:
                print(" ",i,". ", collection)
                i=i+1
            
            print(" ",i,". ","NEW DATABASE")

            num=int(input("Enter your choice: "))

            if num==i:
                dataset_name="new"

            else:
                dataset_name = mongodb_collections[num-1]

            print(dataset_name)
            
            #Read a hole folder from local
            # dc.read_folder_to_hbase(folder_url,dataset_name)
            # print("Adding data from a File System")

        else:
            print("Wrong input")

    elif action == '2':
        print("Consulting existing tables")

    else:
        print("Wrong input")
 

    







    

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
    # connection = happybase.Connection('192.168.1.112', port=9090)  # Assuming default HBase Thrift server port is 9090
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