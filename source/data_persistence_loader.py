import happybase
import pymongo
import os
import json
import csv
from datetime import datetime


# To load the csv
def process_csv(connection, db, table_name, dataset_name): #ask the connection to hbase, the database, the collection name and the table name
    table = connection.table(table_name)    #get the table in hbase
    collection = db[dataset_name]   #Get a collection we will insert the data
    
    #Scan of each row of the data stored in hbase table
    for key, data in table.scan():
        document = { #for each row we create a document
            'name': dataset_name + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),  #with the id the year, and date the data has been added
            'data': {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()} #the rest of the data
        }
        collection.insert_one(document) #insert the document into mongo


# To load jsons files
def process_json(connection, db, table_name, dataset_name):
    table = connection.table(table_name) #get the table name from hbase
    collection = db[dataset_name] #get the collection name 
    
    for key, data in table.scan(): #scan the json in hbase
        # Get all the info of each row of the json in hbase
        property_data = {k.decode('utf-8').split(':')[1]: v.decode('utf-8') for k, v in data.items()}

        # Create a dicument in mongo
        document = {
            'name': dataset_name + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), #the id is the date we added th data
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

            process_csv(connection, db, table_name_str,mongo_dataset_name)  #execute the function to load all table into mongo


        elif table_name_str.startswith('json_table_'): #if the table detected is a csv           
            dot_index = table_name_str.find('.')
            process_json(connection, db, table_name_str, mongo_dataset_name)  #execute the function to load all table into mongo

        print(table_name_str + " added to Persistent Landing")
    # Close connections
    connection.close()

    mongo_client.close()

