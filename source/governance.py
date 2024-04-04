import data_collectors as dc
import data_persistence_loader as dpl
import os
import pymongo
import happybase


#Define all the MongoDB variables to connect
# MongoDB host and port
mongodb_host = 'localhost'
mongodb_port = 27017

#Default name for mongo_db database
database_name="Persistent_Landing"


#Define all the Hbase variables to connect
# HBase host
hbase_host = '192.168.1.41'  # Replace with your HBase host IP address
hbase_port = 9090


def delete_data_from_collection(host, port, database_name, collection_name):
    # Establish a connection to MongoDB
    client = pymongo.MongoClient(host, port)
    db = client[database_name]

    # Delete the collection
    db[collection_name].drop()

    print("Collection '{}' deleted successfully.".format(collection_name))

    # Cerrar la conexión
    client.close()


def delete_temporal_database(hbase_host, hbase_port):
    connection = happybase.Connection(hbase_host,hbase_port)
    for table_name in connection.tables():
        connection.disable_table(table_name)
        connection.delete_table(table_name)
        print(f"Table '{table_name}' deleted from Temporal Landing")

    connection.close()


def consult_temporal_landing(host,port):
    connection = happybase.Connection(host, port=port)
    print(connection.tables())


def consult_persistent_landing(host,port):
    client = pymongo.MongoClient(host, port)
    for db in client.list_database_names():
        print(db)
        database= client[db]
        collections = database.list_collection_names()
        for coll in collections:
            print("   "+coll)
            # collection=database[coll]
            # cursor=collection.find()
            # for document in cursor:
            #     print(document["name"])



def get_data_from_collection(host, port, database_name, collection_name):
    # Establish connection with MongoDB
    client = pymongo.MongoClient(host, port)
    db = client[database_name]

    # Acceder a la colección
    collection = db[collection_name]

    # Recuperar todos los documentos dentro de la colección
    documents = collection.find().limit(10)

    # Iterar sobre los documentos e imprimirlos
    for document in documents:
        print(document)

    # Cerrar la conexión
    client.close()







def get_mongodb_collections():
    # Connect to MongoDB
    client = pymongo.MongoClient(mongodb_host, mongodb_port)

    # List available collections (tables)
    # Access the database
    db = client[database_name]

    # List all collections in the database
    collections = db.list_collection_names()

    # Print the list of collections
    # print("Collections in the 'Persistent_Landing' database:\n", collections)


    # Close the connection
    client.close()

    return(collections)



def add_new_mongo_collection(host,port,database_name,collection_name):
    # Connect to MongoDB
    client = pymongo.MongoClient(host, port)  # Replace 'localhost' with your MongoDB server's address if it's different
    db = client[database_name]

    # Create a new collection
    db.create_collection(collection_name)

    # Close the connection
    client.close()

    print("Collection '{}' created successfully in the database '{}'".format(collection_name, database_name))




def ask_collection(mongodb_collections):
    print("\nSelect the name of the COLLECTION from the list in MongoDB:") #Ask in which collection wants to add this data

    i=1
    for collection in mongodb_collections:
        print(" ",i,". ", collection)
        i=i+1
    
    print(" ",i,". ","NEW DATABASE") #Select new collection or existing one

    num=int(input("Enter your choice: "))

    if num==i: #If we pick a new collection, write its name
        name=input("Enter the name of the new COLLECTION: ")
        dataset_name=name
        add_new_mongo_collection(mongodb_host,mongodb_port,database_name,name)


    else:
        dataset_name = mongodb_collections[num-1]

    return dataset_name






def main():

    # List of collections in MongoDB
    mongodb_collections=get_mongodb_collections()

    #Ask the user what action does he want to perform
    action_text= "Select what action you want to perform: \n 1. Add new data \n 2. Consult existing tables \n 3. Delete tables\nEnter your choice: "
    action=input(action_text)

    if action == '1': #If the user decides to add data
        action_text = "\nSelect the source of the new data you want to add:\n 1. REST API\n 2. File System\nEnter your choice: "
        action = input(action_text) #ask if the data is from API or local system

        if action == '1': #From the APIs
            action_text = "\nInsert the URL (insert 0 if you want the default API):  "
            action=input(action_text)
            if action == '0': 
                url='https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=d7cf9683-5e2d-4b7b-8602-0bc5073f1dc3'
            else: url = action

            #Ask what collection of mongodb does the user want to insert the files
            dataset_name=ask_collection(mongodb_collections)

            dc.add_files_from_api(dataset_name,hbase_host,hbase_port,url) #Get API files from the url and load them into Hbase
            print("Files Succesfully added to temporal landing!")

        elif action == '2': #if the data to be added is from local system
            action_text = "\nInsert the folder URL: "
            
            folder_url = input(action_text) #enter the path of the files

            while not os.path.exists(folder_url):#if the path does not exist, show an error
                print("This url does not exist")
                folder_url = input(action_text)

            #Ask what collection of mongodb does the user want to insert the files
            dataset_name=ask_collection(mongodb_collections)

            dc.add_folder_files_to_hbase(dataset_name,folder_url,hbase_host, hbase_port) #then add all the files from the folder into hbase

            print("Files Succesfully added to temporal landing!")

        else:
            print("Wrong input")
        

        dpl.from_hbase_to_mongo(database_name,mongodb_host,mongodb_port, hbase_host, hbase_port) #once are in the temporal zone, add them into persistent zone

        print("Files Succesfully added to persistent landing!")

        delete_temporal_database(hbase_host, hbase_port) #then delete them from the temporal
        

    elif action == '2': #If the user decides to consult existing data
        #Ask the database to consult
        action_text= "Select what database you want to consult: \n 1. Temporal Landing \n 2. Persistent Landing \nEnter your choice: "

        action=input(action_text)

        if action=='1': #consult the temporal landing
            consult_temporal_landing(hbase_host,hbase_port)
        elif action=='2': #consult the persistent landing
            consult_persistent_landing(mongodb_host,mongodb_port)
            
            print("\nSelect a databaset to consult from the persistent Landing: ")
            i=1 #inside the dataset pick the collection to consult
            for collection in mongodb_collections:
                print(" ",i,". ", collection)
                i=i+1
            num=int(input("Enter your choice: "))
            dataset_name = mongodb_collections[num-1]
              #get the data from the specific collection
            get_data_from_collection(mongodb_host, mongodb_port, database_name, dataset_name)
        else: print("Wrong input")


    elif action == '3': #if the user decides to delete data
        action_text= "Select from what database you want to delete: \n 1. Temporal Landing \n 2. Persistent Landing \nEnter your choice: "

        action=input(action_text)
        #Select the databese in which the data will be deleted
        if action=='1': #delete from temporal
            delete_temporal_database(hbase_host,hbase_port)
        elif action=='2':
            print("\nSelect a databaset to consult from the persistent Landing: ")
            i=1
            for collection in mongodb_collections: #select the collection inside the database to delete
                print(" ",i,". ", collection)
                i=i+1
            num=int(input("Enter your choice: "))
            dataset_name = mongodb_collections[num-1]
              #delete the selected collection
            delete_data_from_collection(mongodb_host, mongodb_port, database_name, dataset_name)
        else: print("Wrong input")
    else:
        print("Wrong input")
 

    

if __name__ == "__main__":
    main()