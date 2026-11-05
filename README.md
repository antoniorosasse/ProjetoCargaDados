# ProjetoCargaDados

ETL de dados públicos de CNPJ da Receita Federal. Lê arquivos Parquet
em chunks e carrega no PostgreSQL e/ou MongoDB.

Este repositório **não inclui os dados**. Ele é a ferramenta que popula
o banco `bancocnpj` consumido pelo [cnpj_interface](https://github.com/antoniorosasse/cnpj_interface).

## Índice

- [Stack](#stack)
- [Estrutura](#estrutura)
- [Requisitos de dados](#requisitos-de-dados)
- [Pré-requisitos](#pré-requisitos)
- [Como rodar local](#como-rodar-local)
- [Ordem de execução (primeira carga)](#ordem-de-execução-primeira-carga)
- [Comandos de ETL](#comandos-de-etl)
  - [Importar Parquet pra Postgres ou Mongo](#importar-parquet-pra-postgres-ou-mongo)
  - [Exemplos práticos](#exemplos-práticos)
  - [Migrar Postgres -> Mongo](#migrar-postgres---mongo)
  - [Migrar tudo](#migrar-tudo)
  - [Carregar coordenadas de CEP (c_ceps)](#carregar-coordenadas-de-cep-c_ceps)
- [Carregar PESSOAS no IBM i (DB2)](#carregar-pessoas-no-ibm-i-db2)
- [Tabelas de CNPJ suportadas](#tabelas-de-cnpj-suportadas)
- [Controle de arquivos processados](#controle-de-arquivos-processados)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Ambiente](#ambiente)
- [Testes](#testes)
- [Repositórios relacionados](#repositórios-relacionados)

## Stack

- Django 5.1.7
- Python 3.13
- pandas + pyarrow (leitura de Parquet)
- PostgreSQL (psycopg2-binary)
- MongoDB (pymongo)
- JPype + jt400.jar (conexão com AS/400 via JDBC)

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
    │   │   ├── carregar_pessoas_db2.py     # Popula AROSASSE1.PESSOAS no IBM i (DB2)
    │   │   ├── carregar_ceps.py            # Coordenadas/nomes de CEP -> c_ceps
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

Este projeto **não inclui** os Parquet de CNPJ. Você precisa baixar da
fonte oficial ou de um mirror.

De onde baixar:

- **Parquet** (recomendado, é o formato que o projeto lê):
  `https://www.kaggle.com/datasets/jurandifranca/cnpj-brasil`
- **CSV zipado** (fonte oficial, no portal de dados abertos):
  `https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj`

**Nomes de arquivo esperados** (o importador faz `glob` por estes padrões):

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

O importador também aceita variantes sem o prefixo `C_` (ex:
`Empresas0.parquet`). Os arquivos devem estar **todos na mesma pasta**.

## Pré-requisitos

- Python 3.13
- PostgreSQL 15 rodando em `localhost:5432` (banco `bancocnpj`)
- MongoDB 8 rodando em `localhost:27017` (só se for usar `--destino mongo`)
- JDK 11+ (pra JVM do JDBC)
- Credenciais de um IBM i (por padrão, `pub400.com`)

## Como rodar local

    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    cp .env.example .env            # preencher valores
    (baixar lib/jt400.jar - ver lib/README.md)
    python manage.py migrate

Conferir as conexões:

    python manage.py testar_postgres
    python manage.py testar_mongo     # so se for usar Mongo
    python manage.py testar_as400

## Ordem de execução (primeira carga)

1. Importar as tabelas de lookup (pequenas, rápidas):

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

O `--tabela` aceita apenas os 10 nomes válidos (`c_cnaes`, `c_empresas`,
`c_estabelecimentos`, `c_motivos`, `c_municipios`, `c_naturezas`,
`c_paises`, `c_qualificacoes`, `c_simples`, `c_socios`). Nome errado
quebra na hora, com mensagem clara.

### Exemplos práticos

Importar `c_estabelecimentos` pro Postgres, só os não-baixados
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

Opções úteis:

- `--destino {postgres,mongo,ambos}` (padrão: ambos)
- `--tabela <nome>` (padrão: todas detectadas)
- `--force` — reprocessa mesmo se já foi importado
- `--limit N` — limita número de arquivos
- `--batch N` — tamanho do lote (padrão: 10000)
- `--progress N` — intervalo de progresso (padrão: 50000)
- `--conflict {update,nothing,none}` — estratégia de conflito
- `--test N` — modo teste (processa apenas N registros)
- `--dump` — gera CSV com registros pulados (apenas para o destino Mongo)
- `--filtro "coluna != valor"` — filtro SQL simples

### Migrar Postgres -> Mongo

    python manage.py migrar_postgres_para_mongo --tabela c_empresas
    python manage.py migrar_postgres_para_mongo --tabela c_empresas --force
    python manage.py migrar_postgres_para_mongo --tabela c_empresas --filtro "cd_sit_cadastro != 8"

### Migrar tudo

    python manage.py migrar_tudo_para_mongo
    python manage.py migrar_tudo_para_mongo --force

## Carregar coordenadas de CEP (c_ceps)

A `c_ceps` guarda coordenada e nome de logradouro por CEP. É o que permite o
link do Google Maps na página de detalhe do
[cnpj_interface](https://github.com/antoniorosasse/cnpj_interface). Três
fontes entram na mesma tabela, cada uma com um papel:

| Fonte | Arquivo | O que traz |
|---|---|---|
| `tbl_cep` | `tbl_cep_201805_csv.csv` (cp1252, vírgula) | nome oficial do logradouro, bairro, município, código IBGE |
| `geocep` | `geocep.csv` (utf-8, `;`) | coordenada **por número** (o loader agrega por CEP) e IBGE |
| `bq` | `bq-results-*.csv` (utf-8, vírgula) | centroide por CEP; é a fonte que cobre os CEPs novos |

Ordem de execução (importa: o **nome** fica com a fonte que carrega primeiro):

    python manage.py carregar_ceps --criar
    python manage.py carregar_ceps --fonte tbl_cep --arquivo /caminho/tbl_cep_201805_csv.csv
    python manage.py carregar_ceps --fonte geocep  --arquivo /caminho/geocep.csv
    python manage.py carregar_ceps --fonte bq      --arquivo /caminho/bq-results-....csv

Resultado local: **1.256.551 CEPs, 1.256.017 com coordenada**, ~62 s por
milhão de linhas (COPY em lotes de 20 mil + `ON CONFLICT (cep) DO UPDATE`).
Cobertura medida em amostra de 774 mil estabelecimentos: **97,5%** com
coordenada.

A coluna **`generico`** marca o CEP que cobre o município inteiro — o Correios
não associa logradouro a ele (`tbl_cep` com `logradouro` vazio, ex.: `89669-000`
Ipira/SC, usado por 809 estabelecimentos em 208 logradouros, a maioria rural).
São **10.218 CEPs** assim, que alcançam **24% dos estabelecimentos** da base
(9,3 milhões, em 5.356 municípios). Nesses CEPs a coordenada é o centroide da
área do município e o pino cai no interior (o `geocep` do `89669-000` até tem
8 pontos por número, mas todos com `numero=0`); por isso o `cnpj_interface`
usa o **endereço em texto** quando `generico = true`. Só o `tbl_cep` marca o
flag — `geocep` e `bq` nunca marcam.

Como o conflito é resolvido:

- `logradouro`, `bairro`, `id_municipio`, `nome_municipio`, `uf` — `COALESCE`:
  nunca apagam o que já está gravado (daí o `tbl_cep` entrar primeiro);
- `generico` — `c_ceps.generico OR EXCLUDED.generico`: uma vez marcado, fica
  marcado (só o `tbl_cep` marca, porque só ele sabe que o CEP não tem
  logradouro);
- `latitude`/`longitude` — só são sobrescritas por fonte de prioridade maior
  ou igual (`PRIORIDADE = {tbl_cep: 1, geocep: 2, bq: 3}`), então a ordem das
  coordenadas não depende de quem rodou por último. A prioridade saiu de
  medição em 1M de CEPs comuns: mediana de 64 m (`bq` × `tbl_cep`), 125 m
  (`bq` × `geocep`) e 169 m (`geocep` × `tbl_cep`); o p99 mostra a cauda de
  erro de cada fonte (21 km, 637 km e 1449 km) — o `tbl_cep` (2018) tem erro
  grosseiro em ~1% dos CEPs e por isso fica por último.

Opções:

- `--criar` — roda o DDL (`CREATE TABLE IF NOT EXISTS c_ceps ...` +
  `ADD COLUMN IF NOT EXISTS generico`, para atualizar tabela antiga) e sai
- `--fonte {tbl_cep,geocep,bq}` + `--arquivo <csv>` — carga de uma fonte
- `--batch N` (padrão: 20000) e `--progress N` (padrão: 200000)
- `--test N` — lê só N linhas (ensaio; grava o que leu)

Sem PostGIS: a coordenada é `numeric(9,6)`, suficiente para o pino no mapa.
Consulta por raio (`ST_DWithin`) exigiria PostGIS + coluna `geography` numa
fase futura.

## Carregar PESSOAS no IBM i (DB2)

O `carregar_pessoas_db2` popula a tabela `AROSASSE1.PESSOAS` do IBM i com dados
**sintéticos**, montados a partir dos sócios PF (`c_socios`) e do endereço da
matriz (`c_estabelecimentos` + `c_municipios`) no Postgres. É o comando que
alimenta o [as400_interface](https://github.com/antoniorosasse/as400_interface),
que lê essa tabela por JDBC.

    # Dry-run: gera e imprime, sem INSERT no DB2
    python manage.py carregar_pessoas_db2 --quantidade 10 --test

    # Carga real
    python manage.py carregar_pessoas_db2 --quantidade 1000000 --batch 500

Opções:

- `--quantidade N` (obrigatório) — quantos registros inserir
- `--batch N` — tamanho do lote no JDBC (padrão: 2000)
- `--progress N` — progresso a cada N registros (padrão: 10000)
- `--seed N` — seed do gerador aleatório (carga reproduzível)
- `--test` — dry-run, não faz INSERT

## Tabelas de CNPJ suportadas

| Tabela | O que e | Linhas aprox. |
|---|---|---|
| c_cnaes | Códigos CNAE | 1.360 |
| c_empresas | Dados cadastrais | 36.9M |
| c_estabelecimentos | Endereços, situação, CNAE | 38.5M |
| c_motivos | Motivos de situação cadastral | 65 |
| c_municipios | Municípios (IBGE) | 5.570 |
| c_naturezas | Naturezas jurídicas | 90 |
| c_paises | Países | 260 |
| c_qualificacoes | Qualificações de sócio | 70 |
| c_simples | Optantes Simples/MEI | 30.3M |
| c_socios | Sócios das empresas | 18.9M |
| c_ceps | Coordenada/nome por CEP (fontes tbl_cep, geocep, bq) | 1.26M |

## Controle de arquivos processados

O `ControleArquivos` registra cada arquivo processado na tabela
`etl_arquivoprocessado` (Postgres). Se o arquivo já foi importado com
sucesso, ele e pulado. Use `--force` pra reprocessar.

## Variáveis de ambiente

Ver `.env.example`. As principais:

- `SECRET_KEY`, `DEBUG`, `ALLOWED_HOSTS` — Django padrão
- `AS400_*` — conexão com o IBM i (obrigatório mesmo se não for usar o AS/400 diretamente: o `settings.py` valida no boot)
- `AS400_JDBC_JAR` — caminho absoluto pro `jt400.jar`
- `PG_*` — conexão com o PostgreSQL (`bancocnpj`)
- `MONGO_*` — conexão com o MongoDB

## Ambiente

- **Local:** apenas (não tem produção)
- **Parquet:** os arquivos ficam fora do projeto (ex:
  `/home/neto/DataSetsTeste/base_cnpj/`)

## Testes

O projeto **não tem testes automatizados** (`etl/tests.py` está vazio). A
validação é feita pelos comandos `testar_*` (que conferem as conexões) e pelo
`--test N` do `importar_cnpj`, que processa apenas N registros antes da carga real.

## Repositórios relacionados

- [cnpj_interface](https://github.com/antoniorosasse/cnpj_interface) — consulta os dados carregados
