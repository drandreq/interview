#!/bin/bash
set -e

echo "----------------------------------------------------"
echo "SCRIPT DE INICIALIZAÇÃO DE BANCOS DE DADOS"
echo "Usuário Principal do Postgres (POSTGRES_USER): $POSTGRES_USER"
echo "Banco de Dados Principal (POSTGRES_DB): $POSTGRES_DB"
echo "----------------------------------------------------"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Verificar e criar database 
    SELECT 'CREATE DATABASE $POSTGRES_OMOP_DB_ENV'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$POSTGRES_OMOP_DB_ENV')\gexec
    GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_OMOP_DB_ENV TO "$POSTGRES_USER";
    \echo "---> Banco de dados '$POSTGRES_OMOP_DB_ENV' verificado/criado e privilégios concedidos para o usuário '$POSTGRES_USER'."


EOSQL

# Adicionado posteriormente devido erro de schema publico para usuario não padrão
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "test_de_ex" <<-EOSQL
#     --GRANT CREATE, USAGE ON SCHEMA public TO debug;
#     --\echo "---> Privilégios CREATE e USAGE NO SCHEMA 'public' do banco 'test_de_ex' concedidos para 'debug'."
# EOSQL

echo "----------------------------------------------------"
echo "SCRIPT DE INICIALIZAÇÃO DE BANCOS DE DADOS CONCLUÍDO"
echo "----------------------------------------------------"