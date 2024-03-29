import happybase
import pymongo
import os
import json
import csv
from datetime import datetime

# Connection to HBase
connection = happybase.Connection('192.168.1.47', port=9090)

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
db = mongo_client['Test'] #The DB i created is called test

# To load the csv
def process_csv(file_path, table_name, year):
    table = connection.table(table_name)    #get the table in hbase
    collection = db['opendatabcn-income']   #Get a collection called opendatabcn-income, we should a except try to check if there is
    
    #Scan of each row of the data stored in hbase table
    for key, data in table.scan():
        document = { #for each row we create a document
            'year': year, #with the id the year
            'data': {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()} #the rest of the data
        }
        collection.insert_one(document) #insert the documento into mongo

# To load jsons
def process_json(table_name, fecha):
    table = connection.table(table_name)
    collection = db['idealista']
    
    for key, data in table.scan():
        # Get all the info of each row of the json in hbase
        property_data = {k.decode('utf-8').split(':')[1]: v.decode('utf-8') for k, v in data.items()}

        # Create a dicument in mongo
        document = {
            'date': fecha, #the id is the date
            'data': property_data #and the res is the data
        }

        # Insert the document in mongo
        collection.insert_one(document)





# Get all the table names of hbase
for table_name in connection.tables():
    table_name_str = table_name.decode('utf-8') #decode the name
    print(table_name_str) 

    if table_name_str.startswith('csv_table_'): #if the table detected is a csv

        year = int(table_name_str.split('_')[2]) #Get the year of the name
        process_csv('/ruta/a/tu/archivo.csv', table_name_str, year)  #execute the function to load all table into mongo

    elif table_name_str.startswith('json_table_'): #if the table detected is a csv
        
        try:
            date_parts = table_name_str.split('_')[2:5]  #Get the date
            date_str = '_'.join(date_parts)
            process_json(table_name_str, date_str)  #execute the function to load all table into mongo
        except ValueError as e:
            print(f"Error al procesar la fecha del nombre de la tabla {table_name_str}: {e}")

# Close connections
connection.close()
mongo_client.close()
