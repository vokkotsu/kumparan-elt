import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker('id_ID') # Use Indonesian locale

# Connection to local PostgreSQL (Docker)
conn = psycopg2.connect(
    host="localhost",
    database="kumparan_db",
    user="admin",
    password="adminpassword",
    port="5432"
)
cursor = conn.cursor()

def generate_articles(num_records):
    print(f"Starting generation of {num_records} articles...")
    
    # Time range from 2016 to now
    start_date = datetime(2016, 1, 1)
    end_date = datetime.now()
    
    insert_query = """
    INSERT INTO articles (title, content, published_at, author_id, created_at, updated_at, deleted_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    records = []
    for i in range(num_records):
        # Generate random date between 2016 - now
        created_at = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        # Updated_at at least equal to created_at, at most now
        updated_at = fake.date_time_between(start_date=created_at, end_date=end_date)
        
        published_at = created_at + timedelta(hours=random.randint(1, 48))
        
        # Simulate 5% of articles being soft-deleted
        is_deleted = random.random() < 0.05
        deleted_at = fake.date_time_between(start_date=published_at, end_date=end_date) if is_deleted else None

        record = (
            fake.sentence(nb_words=6),
            fake.text(max_nb_chars=500),
            published_at,
            random.randint(1, 1000), # random author_id
            created_at,
            updated_at,
            deleted_at
        )
        records.append(record)
        
        # Insert in batches (1000 rows) for speed
        if len(records) == 1000:
            cursor.executemany(insert_query, records)
            conn.commit()
            records = []
            print(f"{i + 1} rows successfully inserted...")

    # Insert remaining data
    if records:
        cursor.executemany(insert_query, records)
        conn.commit()

    print("Data generation complete!")

# Generate 10,000 initial records
generate_articles(10000)

cursor.close()
conn.close()