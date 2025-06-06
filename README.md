Olá! Esta é minha submissão da primeira etapa do desafio proposto.

Os passos foram realizados na seguinte ordem principal:

### 1: Criação do ambiente virtual do python (windows):
````bat
mkdir interview
cd interview
python -m venv venv_interview
venv_interview\Scritps\Activate
pip install -r requirements.txt
````

### 2: Subir Postgres e minIO:
Depende de:
- ps: inicializador do banco de dados postgresql (caso necessário) em ```postgres-init-scripts\01-init-databases.sh```
- Criação de pastas para bind do conteiner docker:
```bat
mkdir binds\minio
mkdir application
```
- Executar o docker-compose.yml
```bat
docker-compose.yml up -d
```
- Executar scripts para carga de dados fake. Os arquivos disponibizados pro desafio foram adequados a erros que surgiram durante testes.
```bat
python application\aq_generate_fake_dataset.py
python application\aq_upload_csv.py
``` 
- [to improve: criar novos dados fake em CSV para minIO]

### 3: Inicializar formatos DDL para OMOP:
Depende de:
- Criar pasta para receber DDL de omop
```bat
mkdir application\omop_ddl
```
- Copiar DDL .sql da fonte: https://github.com/OHDSI/CommonDataModel/tree/main/inst/ddl/5.4/postgresql
- Executar script aq_OMOPCDM_INIT.py para carregar as DDL para o postgres no banco OMOP indicado no .env

### 4: Subir Airflow:
Depende de:
- Pastas pro airflow:
```bat
mkdir airflow_
mkdir airflow_/config
mkdir airflow_/dags
mkdir airflow_/logs
mkdir airflow_/plugins
```
- Executar docker-composer do airflow em separado:
```bat
cd airflow_
docker-compose.yaml
cd ..
```
### 5: Obter os vocabulários OMOP:
Depende de:
- Obter vocabulários OMOP (especialmente *CONCEPTS.CSV* advindo do SNOMED, e/ou incluir demais Vocabulários Provider, ABMS, HES, MEDICARE, NUCC, etc...) do Athena. https://athena.ohdsi.org/
- Descompactar na pasta application\vocabulary
```bat
mkdir application\vocabulary
```
- [To improve] Carregar vocabulários no postgres no banco de dados omop através do arquivo ```application\vocabulary\aq_OMOPCDM_load_vocabulary.py``` [com erro]

### 6: Extrair dados do postgres:
- [To improve: obter TODAS as tabelas de um database do postgresql, referencia data_killer.sql]
- Executar script para obter uma tupla com os dataset de Providers e Care_site
```bat
python application\postgres_extractor.pys
```



## Exercise 2 - Architecture:
Não deu tempo de elaborar este.

Em poucas palavras o que consegui pensar:
- Me parece que a Promptly não pode/deve acessar os dados dos clientes, logo uma imagem de VM pronta end-to-end para todo processo poderia resolver alguns itens de problemas.
- Este "docker-composer" seria inserido na VM cliente e executaria a criação de todas as instancias necessárias para manipular todos os dados - parecido com o que tentei fazer acima - com a diferença que ela precisaria mapear todas as colunas, de todas tabelas de todos os datasets disponíveis (tenho um código que faz isso) porém ainda seria necessário o mapeamento para padrão OMOP.
- Para ampliar as possibilidades de mapeamento poderia ser anexado um modelo pequeno de LLM que rode em CPU para fazer parte do trabalho do mapeamento de conceitos.
- Ao final, os dados seriam testados e a qualidade do resultado seria enviado pro cliente que compartilharia com a promptly de volta.

- [To improve]