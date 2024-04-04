import os
import happybase
import pandas as pd
import json
import urllib3

# Function to load data from a CSV file into an HBase table
def load_csv_to_hbase(path, table_name, connection): #We need to input the path of the filem the name we want to give it in hbase and the hbase conection
    df = pd.read_csv(path)    # Read the CSV file into a pandas DataFrame
    
    if not handle_existing_table(table_name, connection):
        return
    table = connection.table(table_name) #Access the table in hbase
    
    for index, row in df.iterrows(): #Iterate over the rows of the DataFrame to insert each row in HBase
        row_key = str(index).encode('utf-8')        #Use the row index as the row key 
        data_to_insert = {f"info:{col}".encode('utf-8'): str(value).encode('utf-8') for col, value in row.items()}#Prepare the data for insertion (convert to bytes)
        table.put(row_key, data_to_insert)#Insert the data into the hbase table


# Function to load data from a JSON into an HBase table
def load_json_to_hbase(path, table_name, connection):
    with open(path, 'r') as file: #read json
        data_json = json.load(file)
    df_json = pd.DataFrame(data_json)# Convert JSON data into a DataFrame
    
    if not handle_existing_table(table_name, connection):
        return
    table = connection.table(table_name)# Access the table created in hbase
    
    for index, row in df_json.iterrows():# Iterate over the DataFrame rows and insert each row into HBase
        row_key = str(index).encode('utf-8')
        data_to_insert = {f"info:{col}".encode('utf-8'): str(value).encode('utf-8') for col, value in row.items()}
        table.put(row_key, data_to_insert)#Insert the data into the hbase table

# Function to check table existence and handle user input
def handle_existing_table(table_name, connection):
    encoded_table_name = table_name.encode('utf-8') #we have to use the encoded name to delete the table, if not it wont delete it 

    if encoded_table_name in connection.tables(): #If we have a table with the same exact name
        user_choice = input(f"Table '{table_name}' already exists in temporal landing. Type 1 to keep the original, 2 to replace: ")
        if user_choice == '2': #If we want to delete it
            connection.disable_table(encoded_table_name)
            connection.delete_table(encoded_table_name)
            print(f"Deleted existing table '{table_name}'. Loading new data.")
            connection.create_table(table_name, {'info': dict()}) #Create a table with the new data
        else:
            print(f"Keeping original table '{table_name}'. Skipping file.")
            return False
    
    else: #In case we dont have any table with the name, we create it 
        connection.create_table(table_name, {'info': dict()})
    return True





def add_folder_files_to_hbase(dataset_name,path, hbase_host, hbase_port):
    
    # Create connection with HBase
    connection = happybase.Connection(hbase_host, port=hbase_port)


    for file in os.listdir(path): # Scan all files in the directory to loop trought them
        file_path = os.path.join(path, file)  # Get the path of the file 
        
        if file.endswith(".csv"): # Check if the file is a CSV
            table_name = 'csv_table_' + dataset_name + "." + file.replace('.csv', '').replace(' ', '_').lower() #Create a new table name for the CSV file
            load_csv_to_hbase(file_path, table_name, connection) #Load the CSV  into HBase with this name
            print("file "+ file + " added to Temporal Landing")
            
        
        elif file.endswith(".json"): # Check if the file is a JSON
            table_name = 'json_table_' + dataset_name + "." + file.replace('.json', '').replace(' ', '_').lower() #Create a new table name for the json file
            load_json_to_hbase(file_path, table_name, connection) #Load the json  into HBase with this name
            print("file "+ file + " added to Temporal Landing")


    connection.close()# Close the connection



def add_files_from_api(dataset_name,host,port,url):

    #
    http = urllib3.PoolManager()
    # url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=d7cf9683-5e2d-4b7b-8602-0bc5073f1dc3'
    resp = http.request("GET",url)
    data=json.loads(resp.data.decode('utf-8'))
    real_data=(data['result']['records'])
    table_name= 'json_table_' + dataset_name + "." + "api_file" #Create a new table name for the json file 
    connection = happybase.Connection(host, port=port)


    df_json = pd.DataFrame(real_data)# Convert JSON data into a DataFrame
    if not handle_existing_table(table_name, connection):
        return
    
    # Get the table name from hbase
    table = connection.table(table_name)
    
    # Loop trough the json rows and add them to hbase
    for index, row in df_json.iterrows():
        row_key = str(index).encode('utf-8') #important, to decode if not, it wont show data
        data_to_insert = {f"info:{col}".encode('utf-8'): str(value).encode('utf-8') for col, value in row.items()}
        table.put(row_key, data_to_insert)  # Insert into hbase

    # Close connection
    connection.close()