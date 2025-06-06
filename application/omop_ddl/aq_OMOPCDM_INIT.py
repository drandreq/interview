import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv
load_dotenv()


DB_HOST = os.getenv( "POSTGRES_HOST_ENV" )
OMOP_DB_NAME = os.getenv( "POSTGRES_OMOP_DB_ENV" )
DB_USER = os.getenv( "POSTGRES_USER_ENV" )
DB_PASSWORD = os.getenv( "POSTGRES_PASSWORD_ENV" )
DB_PORT = os.getenv( "POSTGRES_PORT_ENV" )

OMOP_DDL_PATH="omop_ddl"
# Ordem de execução dos scripts DDL OMOP
DDL_FILES = [
    "OMOPCDM_postgresql_5.4_ddl.sql",
    "OMOPCDM_postgresql_5.4_primary_keys.sql",
    "OMOPCDM_postgresql_5.4_constraints.sql",
    # "OMOPCDM_postgresql_5.4_indices.sql" # Erro de inserção de indexes, contornado via workbench
]

def create_database_if_not_exists(db_name, host, user, password):
    """
    Conecta ao banco de dados padrão (postgres) para criar o banco de dados OMOP se ele ainda não existir.
    """
    conn = None
    try:
        conn = psycopg2.connect(host=host, database="postgres", user=user, password=password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        if not exists:
            print(f"Banco de dados '{db_name}' não encontrado. Criando...")
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"Banco de dados '{db_name}' criado com sucesso.")
        else:
            print(f"Banco de dados '{db_name}' já existe. Conectando a ele.")

        cursor.close()
    except psycopg2.Error as e:
        print(f"Erro ao criar/verificar o banco de dados '{db_name}': {e}")
        raise
    finally:
        if conn:
            conn.close()

def execute_sql_file(cursor, file_path):
    """
    Lê um codigo SQL e executa suas queries.
    Espera-se que o arquivo SQL contenha múltiplas queries separadas por ';'.
    """
    print(f"Executando script: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        commands = sql_script.replace('@cdmDatabaseSchema.', '').split(';')
        for command in commands:
            # command = re.sub(r"/\*.*?\*/", "", command, flags=re.DOTALL)
            if command.strip():
                cursor.execute(command)
        print(f"Script {file_path} executado com sucesso.")
    except Exception as e:
        print(f"Erro ao executar o script {file_path}: {e}")
        raise

def main():
    conn = None
    try:
        create_database_if_not_exists(OMOP_DB_NAME, DB_HOST, DB_USER, DB_PASSWORD)
        print(f"Conectando ao banco de dados: {OMOP_DB_NAME}")
        conn = psycopg2.connect(host=DB_HOST, database=OMOP_DB_NAME, user=DB_USER, password=DB_PASSWORD)
        conn.autocommit = True
        cursor = conn.cursor()
        for ddl_file in DDL_FILES:
            file_path = os.path.join(OMOP_DDL_PATH, ddl_file)
            if not os.path.exists(file_path):
                print(f"Erro: Arquivo não encontrado: {file_path}. Verifique o caminho OMOP_DDL_PATH e se os arquivos estão lá.")
                return
            execute_sql_file(cursor, file_path)
        print("\nTodas as tabelas OMOP DDL foram criadas com sucesso no banco de dados!")

    except psycopg2.Error as e:
        print(f"Erro de conexão ou de banco de dados: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()