import psycopg2
from psycopg2 import Error
import polars as pl # Ou pandas as pd, se preferir
from dotenv import load_dotenv
import os
load_dotenv()


DB_HOST = os.getenv( "POSTGRES_HOST_ENV" )
DB_NAME = os.getenv( "POSTGRES_DB_ENV" )
DB_USER = os.getenv( "POSTGRES_USER_ENV" )
DB_PASSWORD = os.getenv( "POSTGRES_PASSWORD_ENV" )
DB_PORT = os.getenv( "POSTGRES_PORT_ENV" )

def extract_from_postgres():
    """
    Extrai dados das tabelas 'care_site' e 'provider' do PostgreSQL.
    Retorna:
        Uma tupla contendo dois DataFrames (polars.DataFrame):
        (care_site_df, provider_df).
    """
    connection = None
    care_site_df = None
    provider_df = None
    try:
        print(f"Conectando ao PostgreSQL em {DB_HOST}:{DB_PORT}/{DB_NAME}...")
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = connection.cursor()
        print("Conexão com o PostgreSQL estabelecida com sucesso.")

        print("Extraindo dados da tabela 'care_site'...")
        cursor.execute("SELECT care_site_id, care_site_name, care_site_source_value FROM care_site;")
        care_site_records = cursor.fetchall()
        care_site_columns = [desc[0] for desc in cursor.description]

        if care_site_records:
            care_site_df = pl.DataFrame(care_site_records, schema=care_site_columns)
            
            print(f"Extraídos {len(care_site_df)} registros da tabela 'care_site'.")
        else:
            print("Nenhum registro encontrado na tabela 'care_site'.")
            care_site_df = pl.DataFrame(schema=care_site_columns)


        print("Extraindo dados da tabela 'provider'...")
        cursor.execute("SELECT provider_id, provider_name, npi, specialty, care_site, provider_source_value, specialty_source_value, provider_id_source_value FROM provider;")
        provider_records = cursor.fetchall()
        provider_columns = [desc[0] for desc in cursor.description]

        if provider_records:
            provider_df = pl.DataFrame(provider_records, schema=provider_columns)
            print(f"Extraídos {len(provider_df)} registros da tabela 'provider'.")
        else:
            print("Nenhum registro encontrado na tabela 'provider'.")
            provider_df = pl.DataFrame(schema=provider_columns)

        return care_site_df, provider_df

    except (Exception, Error) as error:
        print(f"Erro ao conectar ou extrair dados do PostgreSQL: {error}")
        return None, None
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Conexão com o PostgreSQL fechada.")

if __name__ == "__main__":
    print("--- Testando a extração do PostgreSQL ---")
    care_site_data, provider_data = extract_from_postgres()

    if care_site_data is not None:
        print("\nDados de care_site (primeiras 5 linhas):")
        print(care_site_data.head())
    if provider_data is not None:
        print("\nDados de provider (primeiras 5 linhas):")
        print(provider_data.head())