# ProjetoCargaDados

ETL de dados publicos de CNPJ da Receita Federal. Le arquivos Parquet
em chunks e carrega no PostgreSQL e/ou MongoDB.

Este repositorio **nao inclui os dados**. Ele e a ferramenta que popula
o banco `bancocnpj` consumido pelo [cnpj_interface](https://github.com/antoniorosasse/cnpj_interface).

## Stack

- Django 5.1.7
- Python 3.13
- pandas + pyarrow (leitura de Parquet)
- PostgreSQL (psycopg2-binary)
- MongoDB (pymongo)
- JPype + jt400.jar (conexao com AS/400 via JDBC)

## Estrutura

    ProjetoCargaDados/
    ├── carga_dados/                # Configuracao Django
    │   ├── settings.py
    │   ├── urls.py
    │   └── wsgi.py
    ├── etl/                        # App de ETL
    │   ├── conexoes/
    │   │   ├── as400.py            # Conexao JDBC com AS/400
    │   │   ├── postgres.py         # Conexao psycopg2
    │   │   └── mongo.py            # Conexao pymongo
    │   ├── management/commands/
    │   │   ├── importar_cnpj.py            # Importa Parquet -> Postgres/Mongo
    │   │   ├── migrar_postgres_para_mongo.py
    │   │   ├── migrar_para_mongo.py
    │   │   ├── migrar_tudo_para_mongo.py
    │   │   ├── testar_as400.py
    │   │   ├── testar_mongo.py
    │   │   └── testar_postgres.py
    │   └── processadores/
    │       ├── cnpj_processor.py   # Processa Parquet em chunks
    │       └── controle_arquivos.py  # Controle de arquivos ja processados
    ├── lib/
    │   └── README.md               # Instrucoes pra baixar o jt400.jar
    ├── manage.py
    └── requirements.txt

## Requisitos de dados

Este projeto **nao inclui** os Parquet de CNPJ. Voce precisa baixar da
fonte oficial ou de um mirror.

De onde baixar:

- **Parquet** (recomendado, e o formato que o projeto le):
  `https://www.kaggle.com/datasets/jurandifranca/cnpj-brasil`
- **CSV zipado** (fonte oficial, no portal de dados abertos):
  `https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj`

**Nomes de arquivo esperados** (o importador faz `glob` por estes padroes):

    C_Cnaes*.parquet
    C_Empresas*.parquet
    C_Estabelecimentos*.parquet
    C_Motivos*.parquet
    C_Municipios*.parquet
    C_Naturezas*.parquet
    C_Paises*.parquet
    C_Qualificacoes*.parquet
    C_Simples*.parquet
    C_Socios*.parquet

O importador tambem aceita variantes sem o prefixo `C_` (ex:
`Empresas0.parquet`). Os arquivos devem estar **todos na mesma pasta**.

## Pre-requisitos

- Python 3.13
- PostgreSQL 15 rodando em `localhost:5432` (banco `bancocnpj`)
- MongoDB 8 rodando em `localhost:27017` (so se for usar `--destino mongo`)
- JDK 11+ (pra JVM do JDBC)
- Credenciais de um IBM i (por padrao, `pub400.com`)

## Como rodar local

    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    cp .env.example .env            # preencher valores
    (baixar lib/jt400.jar - ver lib/README.md)
    python manage.py migrate

Conferir as conexoes:

    python manage.py testar_postgres
    python manage.py testar_mongo     # so se for usar Mongo
    python manage.py testar_as400

## Ordem de execucao (primeira carga)

1. Importar as tabelas de lookup (pequenas, rapidas):

       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_cnaes --destino postgres
       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_motivos --destino postgres
       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_municipios --destino postgres
       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_naturezas --destino postgres
       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_paises --destino postgres
       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_qualificacoes --destino postgres

2. Importar as tabelas grandes (demora horas):

       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_empresas --destino postgres
       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_estabelecimentos --destino postgres
       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_simples --destino postgres
       python manage.py importar_cnpj --pasta /caminho/dos/parquet --tabela c_socios --destino postgres

## Comandos de ETL

### Importar Parquet pra Postgres ou Mongo

    python manage.py importar_cnpj --pasta /caminho/dos/parquet
    python manage.py importar_cnpj --pasta /caminho --destino mongo
    python manage.py importar_cnpj --pasta /caminho --tabela c_empresas
    python manage.py importar_cnpj --pasta /caminho --test 1000   # teste com N registros

O `--tabela` aceita apenas os 10 nomes validos (`c_cnaes`, `c_empresas`,
`c_estabelecimentos`, `c_motivos`, `c_municipios`, `c_naturezas`,
`c_paises`, `c_qualificacoes`, `c_simples`, `c_socios`). Nome errado
quebra na hora, com mensagem clara.

### Exemplos praticos

Importar `c_estabelecimentos` pro Postgres, so os nao-baixados
(`cd_sit_cadastro != 8`), em modo teste (100 mil registros):

    python manage.py importar_cnpj \
        --pasta /home/neto/DataSetsTeste/base_cnpj \
        --tabela c_estabelecimentos \
        --destino postgres \
        --force \
        --batch 50000 \
        --conflict none \
        --filtro "cd_sit_cadastro != 8" \
        --test 100000

Importar `c_empresas` pro Postgres, sem filtro:

    python manage.py importar_cnpj \
        --pasta /home/neto/DataSetsTeste/base_cnpj \
        --tabela c_empresas \
        --destino postgres \
        --force \
        --batch 50000 \
        --conflict none

Migrar `c_estabelecimentos` do Postgres pro Mongo:

    python manage.py migrar_postgres_para_mongo \
        --tabela c_estabelecimentos \
        --force \
        --batch 25000 \
        --progress 100000

Opcoes uteis:

- `--destino {postgres,mongo,ambos}` (padrao: ambos)
- `--tabela <nome>` (padrao: todas detectadas)
- `--force` — reprocessa mesmo se ja foi importado
- `--limit N` — limita numero de arquivos
- `--batch N` — tamanho do lote (padrao: 10000)
- `--progress N` — intervalo de progresso (padrao: 50000)
- `--conflict {update,nothing,none}` — estrategia de conflito
- `--test N` — modo teste (processa apenas N registros)
- `--dump` — gera CSV com registros pulados
- `--filtro "coluna != valor"` — filtro SQL simples

### Migrar Postgres -> Mongo

    python manage.py migrar_postgres_para_mongo --tabela c_empresas
    python manage.py migrar_postgres_para_mongo --tabela c_empresas --force
    python manage.py migrar_postgres_para_mongo --tabela c_empresas --filtro "cd_sit_cadastro != 8"

### Migrar tudo

    python manage.py migrar_tudo_para_mongo
    python manage.py migrar_tudo_para_mongo --force

## Tabelas de CNPJ suportadas

| Tabela | O que e | Linhas aprox. |
|---|---|---|
| c_cnaes | Codigos CNAE | 1.360 |
| c_empresas | Dados cadastrais | 36.9M |
| c_estabelecimentos | Enderecos, situacao, CNAE | 38.5M |
| c_motivos | Motivos de situacao cadastral | 65 |
| c_municipios | Municipios (IBGE) | 5.570 |
| c_naturezas | Naturezas juridicas | 90 |
| c_paises | Paises | 260 |
| c_qualificacoes | Qualificacoes de socio | 70 |
| c_simples | Optantes Simples/MEI | 30.3M |
| c_socios | Socios das empresas | 18.9M |

## Controle de arquivos processados

O `ControleArquivos` registra cada arquivo processado na tabela
`etl_arquivoprocessado` (Postgres). Se o arquivo ja foi importado com
sucesso, ele e pulado. Use `--force` pra reprocessar.

## Variaveis de ambiente

Ver `.env.example`. As principais:

- `SECRET_KEY`, `DEBUG`, `ALLOWED_HOSTS` — Django padrao
- `AS400_*` — conexao com o IBM i (obrigatorio mesmo se nao for usar o AS/400 diretamente: o `settings.py` valida no boot)
- `AS400_JDBC_JAR` — caminho absoluto pro `jt400.jar`
- `PG_*` — conexao com o PostgreSQL (`bancocnpj`)
- `MONGO_*` — conexao com o MongoDB

## Ambiente

- **Local:** apenas (nao tem producao)
- **Parquet:** os arquivos ficam fora do projeto (ex:
  `/home/neto/DataSetsTeste/base_cnpj/`)

## Repositorios relacionados

- [cnpj_interface](https://github.com/antoniorosasse/cnpj_interface) — consulta os dados carregados
