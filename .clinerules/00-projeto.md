# Regras do projeto ProjetoCargaDados

Este arquivo e lido automaticamente pelo Cline no inicio de **toda** conversa
neste workspace. Mantenha-o enxuto e versionado no git.

## Onde esta o que

| Assunto | Arquivo |
|---|---|
| Regras globais (valem nos 4 projetos: menu, as400, cnpj, carga) | `~/Documentos/Cline/Rules/00-sistema-arosasse.md` |
| Estado atual, pendencias, decisoes e arquitetura | `/home/neto/Projetos/docs/PROJETOS.md` (secao 7.4) |
| Comandos (carga, dry-run, migrate) | `/home/neto/Projetos/docs/COMANDOS.md` |
| Historico detalhado (blocos 1-32) | `/home/neto/Projetos/docs/HISTORICO.md` |
| Briefing portatil (para outra IA) | `/home/neto/Projetos/docs/TODOS_PROJETOS.md` |

## Idioma e estilo

- Responda sempre em **portugues do Brasil (pt-BR)**.
- Codigo, comentarios e docstrings: **sem acentos**. Prosa em .md com acentos.
- Nao traduza nomes de tabela/coluna da RFB (`c_estabelecimentos`,
  `cnpj_basico`, `cd_matriz_filial`) nem do DB2 (`PESSOAS`).
- Siga o estilo dos arquivos vizinhos (4 espacos, aspas simples, SQL em
  triple-quote quando o arquivo ja usa).

## Stack e arquitetura

- Django **5.1.7**, Python 3.13, venv **`venv`**. SQLite (`db.sqlite3`) **so**
  para o admin.
- Pacote Django: **`carga_dados/`** (`urls.py` registra apenas `admin/`).
- **Nao existe interface web**: `etl/models.py` esta vazio e `etl/views.py` so
  tem boilerplate. Tudo acontece por `manage.py <comando>`.
- `etl/conexoes/`: `postgres.py` (psycopg2, usa `PG_*` do settings),
  `mongo.py` (pymongo), `as400.py` (JPype + `lib/jt400.jar`).
- `etl/processadores/`: `cnpj_processor.py` (le Parquet em chunks com
  pandas/pyarrow e grava em lote), `controle_arquivos.py` (marca arquivos ja
  processados).
- `etl/management/commands/`: `importar_cnpj`, `carregar_pessoas_db2`,
  `migrar_para_mongo`, `migrar_postgres_para_mongo`, `migrar_tudo_para_mongo`,
  `testar_as400`, `testar_mongo`, `testar_postgres`.
- `carga_dados/settings.py` **exige** `.env` completo (`SECRET_KEY`, `AS400_*`,
  `PG_*`, `MONGO_*`) e levanta `ValueError` se faltar — e proposital: todo
  comando precisa do `.env` real para subir.

## Comandos do projeto

    source venv/bin/activate

    # Importa Parquet da RFB (default: postgres)
    python manage.py importar_cnpj --pasta <dir> [--destino postgres|mongo|ambos] \
        [--tabela c_estabelecimentos] [--test] [--batch 10000] \
        [--conflict update|nothing|none] [--force]

    # DB2: dry-run (nao insere) e carga real (aborta sozinha em SQL0904 = cota cheia)
    python manage.py carregar_pessoas_db2 --quantidade 100000 --test
    python manage.py carregar_pessoas_db2 --quantidade 500000 --batch 2000 --progress 10000

    # Replica Postgres -> Mongo (usado pelo chat do cnpj)
    python manage.py migrar_tudo_para_mongo

- Use sempre o Python do `venv/`. Nao altere `requirements.txt` sem pedir.
- `etl/tests.py` esta vazio: a validacao pratica e `--test` / `--quantidade 100`
  em dev, com o resultado relatado.

## Regras de dados e seguranca

- **Nunca** `TRUNCATE`, `DROP` ou recarga completa sem confirmacao explicita: a
  `bancocnpj` e a fonte do site em producao. Antes, exigir backup
  (`backup_bancos_servidor.sh`) e relatar o volume afetado.
- `carregar_pessoas_db2` grava no DB2 remoto do pub400, cuja cota e ~140 MB:
  `PESSOAS` fica limitada a **500.000 linhas** (IDs 1.770.681-2.270.680). Nao
  liberar `CEPSGEN`.
- `cnpj_dv` e **texto** com `zfill(2)` (o DV `05` nao pode virar `5`).
- `importar_cnpj` e idempotente por `--conflict`; `etl_arquivoprocessado` evita
  reprocessar arquivo. Nao burlar esse controle (`--force` so com motivo).
- Nunca commitar `.env`, `lib/jt400.jar` (driver IBM), `dumps/` ou `backups/`.
- Proibido criar arquivos de backup manuais (`*.bak`) no repo.

## Convencoes de implementacao

- Mudanca de schema/coluna da RFB: ajustar o processador **e**
  `chat/services/schema_context.py` do `cnpj_interface` na mesma tarefa.
- Registrar bloco novo em `docs/HISTORICO.md` + 3 linhas em `docs/PROJETOS.md`
  (secao 2) + comando novo em `docs/COMANDOS.md`.
- Prefira editar arquivos existentes em vez de criar modulos novos.
