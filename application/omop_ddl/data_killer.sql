DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT
            table_schema,
            table_name
        FROM
            information_schema.tables
        WHERE
            table_type = 'BASE TABLE'
            AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            AND table_schema NOT LIKE 'pg_temp_%'
            AND table_schema NOT LIKE 'pg_toast_temp_%'
            AND table_schema = 'public'
            AND table_catalog = 'omop_database'
        ORDER BY
            table_schema, table_name
    LOOP
        RAISE NOTICE 'Tentando remover tabela: %.%', quote_ident(r.table_schema), quote_ident(r.table_name);
        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE;', r.table_schema, r.table_name);
        RAISE NOTICE 'Tabela removida: %.%', quote_ident(r.table_schema), quote_ident(r.table_name);
    END LOOP;
    RAISE NOTICE 'Processo de remoção de tabelas concluído.';
END $$;