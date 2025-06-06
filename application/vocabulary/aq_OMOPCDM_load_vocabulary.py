import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

DB_HOST = os.getenv( "POSTGRES_HOST_ENV" )
OMOP_DB_NAME = os.getenv( "POSTGRES_OMOP_DB_ENV" )
DB_USER = os.getenv( "POSTGRES_USER_ENV" )
DB_PASSWORD = os.getenv( "POSTGRES_PASSWORD_ENV" )
DB_PORT = os.getenv( "POSTGRES_PORT_ENV" )

VOCABULARY_PATH = "vocabulary"

VOCABULARY_TABLES = [
    "CONCEPT.csv",
    "CONCEPT_CLASS.csv",
    "DOMAIN.csv",
    "RELATIONSHIP.csv",
    "VOCABULARY.csv",
    "CONCEPT_RELATIONSHIP.csv",
    "CONCEPT_SYNONYM.csv",
    "DRUG_STRENGTH.csv",
    "CONCEPT_ANCESTOR.csv"
]

def load_vocabulary_csv(cursor, file_name, table_name, vocab_path):
    """
    Carrega um arquivo CSV de vocabulário para a tabela OMOP correspondente usando COPY FROM STDIN.
    """
    full_path = os.path.join(vocab_path, file_name)
    if not os.path.exists(full_path):
        print(f"Aviso: Arquivo {full_path} não encontrado. Pulando o carregamento para {table_name}.")
        return

    print(f"Carregando {file_name} para a tabela {table_name}...")
    try:
        with open(full_path, 'r', encoding='utf-8') as file:
            copy_sql = f"""
            COPY {table_name} FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER E'\t',
                NULL '',
                HEADER TRUE,
                QUOTE
            );
            """
            cursor.copy_expert(copy_sql, file)
            # cursor.copy_from(file, table_name, sep='\t', null='', quote='"')
        print(f"Carregamento de {file_name} para {table_name} concluído com sucesso.")
    except Exception as e:
        print(f"Erro ao carregar {file_name} para {table_name}: {e}")
        raise

def main():
    conn = None
    try:
        print(f"Conectando ao banco de dados OMOP: {OMOP_DB_NAME}")
        conn = psycopg2.connect(host=DB_HOST, database=OMOP_DB_NAME, user=DB_USER, password=DB_PASSWORD)
        conn.autocommit = True

        cursor = conn.cursor()
        for vocab_file in VOCABULARY_TABLES:
            table_name = vocab_file.split('.')[0]
            load_vocabulary_csv(cursor, vocab_file, table_name, VOCABULARY_PATH)
        
        print("\nCarregamento de todos os vocabulários concluído!")
        
    except psycopg2.Error as e:
        print(f"Erro de conexão ou de banco de dados durante o carregamento de vocabulários: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante o carregamento de vocabulários: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()