import psycopg2
import random
import string

DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'test_de_ex'
DB_USER = 'debug'
DB_PASSWORD = 'debug'

## <> Bloco adicionado
try:
    print(f"Conecting to database: {DB_NAME} at {DB_HOST}:{DB_PORT} user: {DB_USER}")
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cursor = conn.cursor()
    print("Successful connection!")
except Exception as e:
    print(f"PostgreSQL connection Error: {e}")
    exit()
## </> Bloco adicionado

# Drop existing tables for a fresh start (optional)
cursor.execute("DROP TABLE IF EXISTS provider;")
cursor.execute("DROP TABLE IF EXISTS care_site;")

# Create care_site table with unique constraint on care_site_name
cursor.execute("""
CREATE TABLE IF NOT EXISTS care_site (
    care_site_id SERIAL PRIMARY KEY,
    care_site_name VARCHAR(255) NOT NULL UNIQUE,  -- Adding UNIQUE constraint
    care_site_source_value VARCHAR(50)
);
""")

# Sample care site data
care_sites = [
    ('City Hospital', 'CSH01'),
    ('Village Clinic', 'VCL01'),
    ('Metro Medical Center', 'MMC01'),
    ('Suburban Health', 'SH01'),
    ('North Health Institute', 'NHI01'),
    ('Eastside Clinic', 'EC01'),
    ('Downtown Health', 'DH01'),
    ('Westside Family Practice', 'WFP01'),
]

# Insert care site data into the table
for care_site in care_sites:
    cursor.execute("""
    INSERT INTO care_site (care_site_name, care_site_source_value) 
    VALUES (%s, %s) 
    ON CONFLICT ON CONSTRAINT care_site_care_site_name_key DO NOTHING;  -- Using the unique constraint
    """, care_site)

# Create provider table
cursor.execute("""
CREATE TABLE IF NOT EXISTS provider (
    provider_id SERIAL PRIMARY KEY,
    provider_name VARCHAR(255) NOT NULL,
    npi VARCHAR(10) UNIQUE NOT NULL, -- National Provider Identifier (should be unique)
    specialty VARCHAR(100), -- Provider specialty
    care_site VARCHAR(255), -- Care site where the provider practices
    provider_source_value VARCHAR(50), -- Source value for the provider identifier
    specialty_source_value VARCHAR(50), -- Source value for the specialty
    provider_id_source_value VARCHAR(50) -- Source value for the provider identifier
);
""")

# Function to generate a random NPI
def generate_npi():
    return ''.join(random.choices(string.digits, k=10))

# Function to generate random provider names
def generate_provider_name():
    first_names = ['John', 'Jane', 'Emily', 'Michael', 'Sarah', 'Robert', 'Linda', 'Kevin', 'Patricia', 'Laura']
    last_names = ['Doe', 'Smith', 'Johnson', 'Brown', 'Wilson', 'Garcia', 'Martinez', 'Lee', 'Rodriguez', 'Davis']
    return f"{random.choice(first_names)} {random.choice(last_names)}"

# Insert 2 million provider rows into the table
for _ in range(2000000):
    provider_name = generate_provider_name()
    npi = generate_npi()
    specialty = random.choice(['Cardiology', 'Pediatrics', 'Neurology', 'Oncology', 'Dermatology', 'Orthopedics', 'Internal Medicine', 'General Practice'])
    care_site = random.choice(care_sites)[0]  # Randomly choose a care site name from the care_sites list
    provider_source_value = provider_name.split()[0][0] + provider_name.split()[1]  # First initial + last name
    specialty_source_value = specialty
    provider_id_source_value = f"{provider_name.split()[0][0]}-{npi}"  # First initial + NPI

    cursor.execute("""
    INSERT INTO provider (provider_name, npi, specialty, care_site, provider_source_value, specialty_source_value, provider_id_source_value) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT provider_npi_key DO NOTHING;  -- Avoid inserting duplicate NPI values
    """, (provider_name, npi, specialty, care_site, provider_source_value, specialty_source_value, provider_id_source_value))

# Commit changes and close the connection
conn.commit()
cursor.close()
conn.close()
print("Database populated successfully!")
