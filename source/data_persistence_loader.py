import happybase
import pymongo
import os
import json
import csv
from datetime import datetime


# To load the csv
def process_csv(connection, db, table_name, dataset_name):
    table = connection.table(table_name)    #get the table in hbase
    collection = db[dataset_name]   #Get a collection called opendatabcn-income, we should a except try to check if there is
    
    #Scan of each row of the data stored in hbase table
    for key, data in table.scan():
        document = { #for each row we create a document
            'name': dataset_name + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),  #with the id the year
            'data': {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()} #the rest of the data
        }
        collection.insert_one(document) #insert the documento into mongo


# To load jsons
def process_json(connection, db, table_name, dataset_name):
    table = connection.table(table_name)
    collection = db[dataset_name]
    
    for key, data in table.scan():
        # Get all the info of each row of the json in hbase
        property_data = {k.decode('utf-8').split(':')[1]: v.decode('utf-8') for k, v in data.items()}

        # Create a dicument in mongo
        document = {
            'name': dataset_name + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), #the id is the date
            'data': property_data #and the res is the data
        }

        # Insert the document in mongo
        collection.insert_one(document)





def from_hbase_to_mongo(mongo_database_name,mongo_dataset_name,mongo_host,mongo_port, hbase_host, hbase_port):

    # Connection to HBase
    connection = happybase.Connection(hbase_host, hbase_port)

    # Connection to MongoDB
    mongo_client = pymongo.MongoClient(mongo_host,mongo_port)
    db = mongo_client[mongo_database_name] #Enter the DB created

    # Get all the table names of hbase
    for table_name in connection.tables():
        table_name_str = table_name.decode('utf-8') #decode the name
         

        if table_name_str.startswith('csv_table_'): #if the table detected is a csv

            # Find the index of the first occurrence of '.' from the end
            dot_index = table_name_str.find('.')

            # Extract the part before the first '.' and after 'csv_table_'

            # dataset_name = table_name_str[table_name_str.find('csv_table_') + len('csv_table_'):dot_index]
            # print(dataset_name)
            # year = int(table_name_str.split('_')[2]) #Get the year of the name

            process_csv(connection, db, table_name_str,mongo_dataset_name)  #execute the function to load all table into mongo


        elif table_name_str.startswith('json_table_'): #if the table detected is a csv
            
            # try:
                # date_parts = table_name_str.split('_')[2:5]  #Get the date
                # date_str = '_'.join(date_parts)
            dot_index = table_name_str.find('.')

            # dataset_name = table_name_str[table_name_str.find('json_table_') + len('json_table_'):dot_index]
            # print(dataset_name)
            process_json(connection, db, table_name_str, mongo_dataset_name)  #execute the function to load all table into mongo

            # except ValueError as e:
            #     print(f"Error al procesar la fecha del nombre de la tabla {table_name_str}: {e}")

        print(table_name_str + " added to Persistent Landing")
    # Close connections
    connection.close()

    mongo_client.close()

