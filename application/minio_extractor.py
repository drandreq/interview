from minio import Minio
from minio.error import S3Error
import polars as pl
import io
import os
from dotenv import load_dotenv
load_dotenv()

MINIO_ENDPOINT = os.getenv( "MINIO_ENDPOINT" )
MINIO_ACCESS_KEY = os.getenv( "MINIO_ACCESS_KEY" )
MINIO_SECRET_KEY = os.getenv( "MINIO_SECRET_KEY" )
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "False").lower() == "true" 
MINIO_BUCKET_NAME = os.getenv( "MINIO_BUCKET_NAME" )

TEMP_DOWNLOAD_DIR = "minio_downloads_temp"

def extract_from_minio():
    """
    Extrai dados de arquivos CSV de um bucket MinIO.
    Baixa os arquivos para um diretório temporário e lê em um DataFrame.

    Retorna:
        DataFrame (polars.DataFrame)
        contendo os dados combinados dos arquivos CSV 
        (considerando que o bucket contenha apenas dados de providers).
        Retorna um DataFrame vazio em caso de erro ou sem arquivos.
    """
    minio_client = None
    all_provider_data = []

    try:
        print(f"Conectando ao MinIO em {MINIO_ENDPOINT}...")
        minio_client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_USE_SSL
        )
        print("Conexão com o MinIO estabelecida com sucesso.")

        if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
            print(f"Erro: O bucket '{MINIO_BUCKET_NAME}' não existe no MinIO.")
            return pl.DataFrame()

        print(f"Listando objetos no bucket '{MINIO_BUCKET_NAME}'...")

        objects = minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True)
        
        os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

        downloaded_files_count = 0
        for obj in objects:
            if obj.object_name.endswith(".csv"):
                print(f"Baixando arquivo: {obj.object_name}")
                try:
                    response = minio_client.get_object(MINIO_BUCKET_NAME, obj.object_name)
                    
                    file_data = response.read()

                    df_file = pl.read_csv(io.BytesIO(file_data))
                    
                    all_provider_data.append(df_file)
                    downloaded_files_count += 1
                except S3Error as err:
                    print(f"Erro MinIO ao baixar '{obj.object_name}': {err}")
                except Exception as err:
                    print(f"Erro ao processar '{obj.object_name}': {err}")
                finally:
                    response.close()
                    response.release_conn()

        if downloaded_files_count == 0:
            print("Nenhum arquivo CSV encontrado ou baixado do MinIO.")
            return pl.DataFrame() 

        print(f"Combinando {downloaded_files_count} DataFrames de provedores do MinIO...")
        combined_df = pl.concat(all_provider_data)
        print(f"Total de registros combinados do MinIO: {len(combined_df)}")

        return combined_df

    except S3Error as err:
        print(f"Erro de S3 (MinIO): {err}")
        return pl.DataFrame()
    except Exception as err:
        print(f"Erro geral ao extrair dados do MinIO: {err}")
        return pl.DataFrame()
    finally:
        import shutil
        if os.path.exists(TEMP_DOWNLOAD_DIR):
            print(f"Removendo diretório temporário: {TEMP_DOWNLOAD_DIR}")
            shutil.rmtree(TEMP_DOWNLOAD_DIR)

if __name__ == "__main__":
    print("--- Testando a extração do MinIO ---")

    minio_provider_data = extract_from_minio()

    if not minio_provider_data.is_empty():
        print("\nDados de provedores do MinIO (primeiras 5 linhas):")
        print(minio_provider_data.head())
    else:
        print("Nenhum dado de provedor extraído do MinIO ou ocorreu um erro.")