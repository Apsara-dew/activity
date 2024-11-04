import pandas as pd
from pymongo import MongoClient

# Step 1: Load CSV Data
csv_file_path = "e_commerce_data.csv"  # Replace with your CSV file path
df = pd.read_csv(csv_file_path)

# Step 2: Connect to MongoDB
# Replace "localhost" and "27017" with your MongoDB host and port if necessary
client = MongoClient("mongodb+srv://apsara_04:Apsa1234@cluster0.c4bj8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

# Create or select the database
db_name = "activity_01"  # Replace with your desired database name
db = client[db_name]

# Create or select the collection
collection_name = "csv"  # Replace with your desired collection name
collection = db[collection_name]

# Step 3: Insert Data into MongoDB
# Convert the DataFrame to dictionary format (list of dictionaries)
data_dict = df.to_dict("records")

# Insert the data into MongoDB
result = collection.insert_many(data_dict)
print(f"Inserted {len(result.inserted_ids)} documents into collection '{collection_name}' of database '{db_name}'")
