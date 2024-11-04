from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb+srv://apsara_04:Apsa1234@cluster0.c4bj8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["activity_01"]  # Replace with your database name
collection = db["csv"]  # Replace with your collection name

# 1. Find the Products Sold the Most
# Aggregate to get the total quantity sold per product
most_sold_product_pipeline = [
    {"$group": {"_id": "$Product", "Total_Quantity": {"$sum": "$Quantity"}}},
    {"$sort": {"Total_Quantity": -1}},  # Sort in descending order
    {"$limit": 1}  # Get the top product
]

# Execute the aggregation
most_sold_product = list(collection.aggregate(most_sold_product_pipeline))
print("Most Sold Product:", most_sold_product)


# 2. Find the Mostly Used Payment Method
# Aggregate to find the most used payment method
most_used_payment_method_pipeline = [
    {"$group": {"_id": "$Payment_Method", "Count": {"$sum": 1}}},
    {"$sort": {"Count": -1}},  # Sort in descending order
    {"$limit": 1}  # Get the top payment method
]

# Execute the aggregation
most_used_payment_method = list(collection.aggregate(most_used_payment_method_pipeline))
print("Most Used Payment Method:", most_used_payment_method)


# 3. Find the Most Profitable Category
# Aggregate to find the most profitable product category
most_profitable_category_pipeline = [
    {"$group": {"_id": "$Category", "Total_Profit": {"$sum": "$Total_Price"}}},
    {"$sort": {"Total_Profit": -1}},  # Sort in descending order
    {"$limit": 1}  # Get the top category
]

# Execute the aggregation
most_profitable_category = list(collection.aggregate(most_profitable_category_pipeline))
print("Most Profitable Category:", most_profitable_category)
