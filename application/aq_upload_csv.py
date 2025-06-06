import io
from minio import Minio
from minio.error import S3Error
import random
import string
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()

MINIO_ENDPOINT = os.getenv( "MINIO_ENDPOINT" )
MINIO_ACCESS_KEY = os.getenv( "MINIO_ACCESS_KEY" )
MINIO_SECRET_KEY = os.getenv( "MINIO_SECRET_KEY" )
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "False").lower() == "true" 

MINIO_BUCKET_NAME = os.getenv( "MINIO_BUCKET_NAME" )
OBJECT_NAME = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_providers.csv"

csv_data_string = """ProviderName,ProviderID,NPI,Specialty,SiteName,SourceID,SpecSource,IDSource
John Doe,001,1234567890,Cardiology,City Hospital,JD001,Cardiology,JD-NPI
Jane Smith,002,2345678901,Peds,Village Clinic,JS002,Pediatrics,JS-NPI
Dr. J. Doe,001,1234567890,Cardiology,City Hospital,JD001,Cardiology,JD-NPI
Johnathan Doe,003,3456789012,Neuro,Metro Med,JD003,Neurology,JD-NPI-NEW
Jane S.,004,NULL,Peds,Suburban Health,JS004,Pediatrics,JS-NPI-SHORT
Dr. John Doe,001,1234567890,Cardiology,City Hospital,JD001,Cardiology,JD-NPI
Emily J,005,6789012345,Oncology,North Health Inst,EJ006,Oncology,EJ-NPI
NULL,006,7890123456,Ortho,Downtown Health,MB007,Orthopedics,MB-NPI
Sarah Wilson,007,8901234567,Dermatology,Metro Med,SW008,Dermatology,SW-NPI
Chris Davis,008,9012345678,General Practice,City Hospital,CD009,General Practice,CD-NPI
Laura Taylor,009,0123456789,Int Med,Village Clinic,LT010,Int Med,LT-NPI
Kevin Garcia,010,1234567800,Cardiology,City Hospital,KG011,Cardiology,KG-NPI
Patricia Martinez,011,2345678909,Peds,Suburban Health,PM012,Pediatrics,PM-NPI
Robert Lee,012,3456789018,Neuro,Eastside Clinic,RL013,Neurology,RL-NPI
Linda R.,013,4567890127,Derma,Downtown Health,LR014,Dermatology,LR-NPI"""

def generate_npi():
    return ''.join(random.choices(string.digits, k=10))

def generate_provider_id():
    return ''.join(random.choices(string.digits, k=5))

def generate_provider_name():
    first_names = ['John', 'Jane', 'Emily', 'Michael', 'Sarah', 'Robert', 'Linda', 'Kevin', 'Patricia', 'Laura']
    last_names = ['Doe', 'Smith', 'Johnson', 'Brown', 'Wilson', 'Garcia', 'Martinez', 'Lee', 'Rodriguez', 'Davis']
    return f"{random.choice(first_names)} {random.choice(last_names)}"

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

def generate_csv_string(rows=15):
    csv = ["ProviderName,ProviderID,NPI,Specialty,SiteName,SourceID,SpecSource,IDSource\n"]
    for _ in range(rows):
        provider_name = generate_provider_name()
        provider_id = generate_provider_id()
        npi = generate_npi()
        specialty = random.choice(['Cardiology', 'Cardio', 'Pediatrics', 'Peds', 'Neurology', 'Neuro', 'Oncology', 'Onco', 'Dermatology', 'Derma', 'Orthopedics', 'Ortho', 'Internal Medicine', 'Int Med', 'General Practice'])
        site_name = random.choice(care_sites)[0]  # Randomly choose a care site name from the care_sites list
        source_id = ''
        provider_source_value = provider_name.split()[0][0] + provider_name.split()[1]  # First initial + last name
        spec_source = specialty
        provider_id_source_value = f"{provider_name.split()[0][0]}-{npi}"  # First initial + NPI

        row_data = [
            provider_name,
            # provider_id_seq,
            npi,
            specialty,
            # care_site_name_choice,
            source_id,
            spec_source,
            # id_source
        ]
        csv.append(",".join(map(str, row_data)) + "\n")
    return "".join(csv)

def main():
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_USE_SSL
        )
        print("Client minIO successfuly initialized.")
    except Exception as e:
        print(f"ERROR in client minIO: {e}")
        return
    
    try:
        csv_bytes = csv_data_string.encode('utf-8')
        csv_stream = io.BytesIO(csv_bytes)
        csv_length = len(csv_bytes)
        print(f"Converted string in bytes with {csv_length} bytes).")
    except Exception as e:
        print(f"ERROR coverting CSV to bytes: {e}")
        return

    try:
        if not client.bucket_exists(MINIO_BUCKET_NAME):
            client.make_bucket(MINIO_BUCKET_NAME)
            print(f"Bucket '{MINIO_BUCKET_NAME}' created.")
        else:
            print(f"Bucket '{MINIO_BUCKET_NAME}' already exists.")

        print(f"Uploading '{OBJECT_NAME}' to bucket '{MINIO_BUCKET_NAME}'...")
        client.put_object(
            MINIO_BUCKET_NAME,
            OBJECT_NAME,
            csv_stream,
            length=csv_length,
            content_type='text/csv'
        )
        print(f"File '{OBJECT_NAME}' sent sucessfuly to bucket '{MINIO_BUCKET_NAME}'.")

    except S3Error as e:
        print(f"ERROR in minIO operation: {e}")
    except Exception as e:
        print(f"Unexpected error occured: {e}")

if __name__ == "__main__":
    main()