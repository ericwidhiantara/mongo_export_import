import csv
import bcrypt
from pymongo import MongoClient
from datetime import datetime
from nanoid import generate

# Connect to MongoDB
client = MongoClient('mongodb+srv://kartika-dev-rubi:PkfrMtSsKieYV6W2@kartikamas.weiak.mongodb.net/?retryWrites=true'
                     '&w=majority&appName=naryama-panel')
db = client['kartikamas']
collection = db['users']


# Function to hash password using bcrypt
def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())


def generate_user_id():
    user_id = generate(size=10).upper().replace('_', '').replace('-', '')
    return user_id


# Function to generate username
def generate_username(full_name, city):
    # Check if the full name contains more than one word
    if len(full_name.split()) > 1:
        # Extract middle name from full name
        middle_name = full_name.split()[1].lower()
    else:
        # Use the entire name if it contains only one word
        middle_name = full_name.lower()
    # Create username

    return f"{middle_name}_agen_{city.lower().replace(' ', '_')}"


# Open and read CSV file
with open('file_1.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:

        # Hash password
        hashed_password = hash_password(row['PASSWORD'])
        # Generate username
        username = generate_username(row['NAMA MITRA'], row['KOTA'])
        # Prepare document to insert into MongoDB
        document = {
            "full_name": row['NAMA MITRA'],
            "no_identitas": "",
            "jenis_kelamin": "",
            "pekerjaan": "",
            "status_perkawinan": "",
            "whatsapp": '62' + row['NO TELEPON'][1:],
            "address": row['ALAMAT'],
            "email": username,
            "password_hash": hashed_password.decode('utf-8'),
            "status": "active",
            "role": "Agen",
            "user_id": generate_user_id(),
            "city": row['KOTA'],
            "created_at": datetime.now(),
            "created_by": "b1047233-d264-4649-ad6e-62523ca70b31"
        }
        # Insert document into MongoDB
        collection.insert_one(document)
        print(f"Inserted: {document}")

print("Import completed.")
