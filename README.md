# ProjetoCargaDados

ETL de dados publicos de CNPJ da Receita Federal. Le arquivos Parquet,
processa em chunks e carrega no PostgreSQL e/ou MongoDB.

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

## Como rodar local

    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    cp .env.example .env            # preencher valores
    (baixar lib/jt400.jar - ver lib/README.md)
    python manage.py migrate
    python manage.py testar_postgres
    python manage.py testar_mongo
    python manage.py testar_as400

## Comandos de ETL

### Importar Parquet pra Postgres ou Mongo

    python manage.py importar_cnpj --pasta /caminho/dos/parquet
    python manage.py importar_cnpj --pasta /caminho --destino mongo
    python manage.py importar_cnpj --pasta /caminho --tabela c_empresas
    python manage.py importar_cnpj --pasta /caminho --test 1000   # teste com N registros

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

## Ambiente

- **Local:** apenas (nao tem producao)
- **Parquet:** /home/neto/DataSetsTeste/base_cnpj/ (fora do projeto)

## Repositorios relacionados

- [cnpj_interface](https://github.com/antoniorosasse/cnpj_interface) — consulta os dados carregados
- [Projetos](https://github.com/antoniorosasse/Projetos) — scripts e manual de git
