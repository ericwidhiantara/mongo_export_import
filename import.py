from pymongo import MongoClient
import json
import os
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()
# Connect to MongoDB
client = MongoClient(os.getenv("MONGODB_URL_IMPORT"))

# Access the database
db = client[os.getenv("MONGODB_COLLECTION_IMPORT")]

# Directory containing JSON files
directory = os.getenv("DIRECTORY")

# Get list of JSON files in the directory
json_files = [file for file in os.listdir(directory) if file.endswith('.json')]

# Import each collection from JSON files
for json_file in json_files:
    # Extract collection name from filename (remove extension and "naryama.")
    collection_name = os.path.splitext(json_file)[0].replace("{directory}.", "")

    # Open the JSON file and read its contents
    with open(os.path.join(directory, json_file), 'r') as file:
        try:
            # Load the entire JSON array from the file
            json_array = json.load(file)

            # Replace $oid with ObjectId in the _id field
            for doc in json_array:
                if '_id' in doc and isinstance(doc['_id'], dict) and '$oid' in doc['_id']:
                    doc['_id'] = ObjectId(doc['_id']['$oid'])

            # Insert documents into the collection
            if json_array:
                db[collection_name].insert_many(json_array)
        except json.decoder.JSONDecodeError as e:
            print(f"Error decoding JSON in {json_file}: {e}")

print("Import complete.")
