import csv
from datetime import datetime
from pymongo.errors import DuplicateKeyError
from connect import db  

CSV_PATH = "data/data.csv"   

# Creamos los índices

def ensure_indexes():
    # users: email único
    db.users.create_index("email", unique=True)

    # tickets: installation_id + created_at
    db.tickets.create_index("installation_id")
    db.tickets.create_index([("created_at", -1)])


# Insertamos usuarios

def insert_user(row):
    users = db.users

    user_doc = {
        "user_id": row["user_id"],
        "expediente": row["expediente"],
        "email": row["email"],
        "password_hash": row["password_hash"],
        "role": row["role"],
        "createdAt": datetime.utcnow(),
    }

    try:
        result = users.update_one(
            {"email": user_doc["email"]},
            {"$setOnInsert": user_doc},
            upsert=True,
        )

        if result.upserted_id is not None:
            print(f"Usuario creado: {user_doc['email']}")
        else:
            print(f"Usuario ya existía: {user_doc['email']}")

    except DuplicateKeyError:
        print(f"⚠ Email duplicado, ignorado: {user_doc['email']}")



# Insertar ticket

def insert_ticket(row):
    tickets = db.tickets

    ticket_doc = {
        "ticket_id": row["ticket_id"],
        "title": row["title"],
        "description": row["description"],
        "category": row["category"],
        "status": row["status"],
        "priority": row["priority"],
        "user_id": row["user_id"],
        "installation_id": row["installation_id"],
        "created_at": datetime.utcnow()
    }

    tickets.insert_one(ticket_doc)
    print(f"Ticket creado: {ticket_doc['ticket_id']} ({ticket_doc['title']})")

# Leer CSV y poblar Mongo
def load_from_csv(csv_file=CSV_PATH):
    ensure_indexes()

    with open(csv_file, newline="", encoding="utf-8") as fd:
        reader = csv.DictReader(fd)

        for row in reader:
            print(f"\nProcesando ticket {row['ticket_id']}...")

            insert_user(row)
            insert_ticket(row)

def main():
    print("=== Populate Mongo ===")
    load_from_csv()


if __name__ == "__main__":
    main()
