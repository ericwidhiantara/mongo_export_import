from dotenv import load_dotenv
from pymongo import MongoClient
import json
import os
from bson import json_util

load_dotenv()
# Connect to MongoDB
client = MongoClient(os.getenv("MONGODB_URL_EXPORT"))

# Access the database
db = client[os.getenv("MONGODB_COLLECTION_EXPORT")]

# Get a list of all collections in the database
collections = db.list_collection_names()

# Directory to save JSON files
directory = os.getenv("DIRECTORY")

# Create the directory if it doesn't exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Export each collection to a JSON file
for collection_name in collections:
    # Create a cursor to iterate over documents in the collection
    cursor = db[collection_name].find()
    length = db[collection_name].count_documents({})

    # Write documents to a JSON file
    with open(os.path.join(directory, f'naryama.{collection_name}.json'), 'w') as file:
        # Start the JSON array
        file.write("[")
        # Iterate over documents
        for i, document in enumerate(cursor):
            # Preprocess document to handle ObjectId
            processed_document = json.loads(json_util.dumps(document))
            # Write processed document to file
            json.dump(processed_document, file)
            # Add comma if not the last document
            if i < length - 1:
                file.write(",\n")
        # End the JSON array
        file.write("]")

print("Export complete.")
