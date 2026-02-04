import pymongo
import pandas as pd

# MongoDB connection string with username and password
mongo_uri = "mongodb+srv://ipc_user:93i8N2o4e1HvAP65@db-mongodb-sfo3-51186-a3cdbca3.mongo.ondigitalocean.com/ipc_db?tls=true&authSource=admin&replicaSet=db-mongodb-sfo3-51186"

from pymongo import MongoClient

# Function to retrieve all documents from the MongoDB collection
def retrieve_orders(mongo_uri, db_name, collection_name):
    # Connect to MongoDB
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=30000)
    db = client[db_name]

    # Access the specified collection
    collection = db[collection_name]

    # Find all documents in the collection with optional limit
    orders = list(collection.find())

    # Convert the retrieved data to a DataFrame
    orders_df = pd.DataFrame(orders)

    return orders_df


db_name = "ipc_db"
collection_name = "timelines"

# Step 1: Retrieve all orders
orders_df = retrieve_orders(mongo_uri, db_name, collection_name)