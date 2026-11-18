#!/usr/bin/env bash
# ============================================================================
# restaura_base_cnpj.sh -- publica na VPS os dumps por tabela gerados por
# `manage.py publicar_base_cnpj` (ProjetoCargaDados).
#
# Roda NA VPS, como o usuario dono do banco (neto). O comando Python envia
# este arquivo junto com os dumps, entao nao ha nada para instalar la.
#
#   bash ~/carga/restaura_base_cnpj.sh --fase restaurar --rotulo 2026-11 \
#        --tabelas c_paises,c_motivos --sem-backup --sem-parar
#
# Fases (cada uma pode ser rodada sozinha, na ordem):
#   ddl        salva o schema de cada tabela (pg_dump --schema-only) e dropa
#              constraints e indices do lote -- libera o COPY
#   restaurar  TRUNCATE RESTART IDENTITY + pg_restore --data-only + setval
#   indices    pg_restore --section=post-data (recria PK/unique/indices) +
#              ANALYZE -- aqui o site ja pode voltar
#   finalizar  sp_recalcular_estatisticas_totais + religa o servico
#   tudo       restaurar + indices + finalizar
#   status     contagens, indices e servico (nao altera nada)
#
# Comunicacao com o comando Python (uma linha por evento, sem acento):
#   ##INICIO|etapa|tabela|detalhe
#   ##ETAPA|etapa|tabela|detalhe|quantidade|duracao_s
#   ##AVISO|texto
# ============================================================================
set -euo pipefail

BASE="${PUBLICACAO_REMOTO:-$HOME/carga}"
ROTULO=""
TABELAS=""
FASE="status"
JOBS="${PUBLICACAO_JOBS:-4}"
PARAR=1
BACKUP=1
SERVICO="${PUBLICACAO_SERVICO:-cnpj}"
PGHOST_="${PGHOST:-localhost}"
PGPORT_="${PGPORT:-5432}"
PGDB="${PGDATABASE:-bancocnpj}"
PGUSER_="${PGUSER:-arosasse}"
MWM="${MWM:-2GB}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fase) FASE="$2"; shift 2 ;;
        --rotulo) ROTULO="$2"; shift 2 ;;
        --tabelas) TABELAS="$2"; shift 2 ;;
        --jobs) JOBS="$2"; shift 2 ;;
        --servico) SERVICO="$2"; shift 2 ;;
        --base) BASE="$2"; shift 2 ;;
        --banco) PGDB="$2"; shift 2 ;;
        --usuario) PGUSER_="$2"; shift 2 ;;
        --host-pg) PGHOST_="$2"; shift 2 ;;
        --porta) PGPORT_="$2"; shift 2 ;;
        --sem-parar) PARAR=0; shift ;;
        --parar) PARAR=1; shift ;;
        --sem-backup) BACKUP=0; shift ;;
        --backup) BACKUP=1; shift ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "opcao desconhecida: $1" >&2; exit 2 ;;
    esac
done

export PGHOST="$PGHOST_" PGPORT="$PGPORT_" PGDATABASE="$PGDB" PGUSER="$PGUSER_"
# .pgpass do usuario: nenhuma senha no script, no log ou no ambiente do Python.
export PGPASSFILE="${PGPASSFILE:-$HOME/.pgpass}"

DUMP_DIR="$BASE/dumps"
DDL_DIR="$BASE/ddl"
LOG_DIR="$BASE/logs"
BACKUP_DIR="$BASE/backups"
mkdir -p "$LOG_DIR" "$BACKUP_DIR" "$DDL_DIR"
LOG="$LOG_DIR/${FASE}_${ROTULO:-sem-rotulo}_$(date +%Y%m%d-%H%M%S).log"

TAB_ARR=()
if [[ -n "$TABELAS" ]]; then
    IFS=',' read -r -a TAB_ARR <<< "$TABELAS"
fi

# ------------------------------------------------------------------ utilidades

agora() { date +%s; }

emit_inicio() { printf '##INICIO|%s|%s|%s\n' "$1" "$2" "$3"; }

emit_etapa() {  # etapa tabela detalhe quantidade duracao
    printf '##ETAPA|%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5"
}

emit_aviso() { printf '##AVISO|%s\n' "$1"; }

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG" >&2; }

sql() {  # executa um comando e devolve o resultado cru (uma linha por linha)
    psql -X -q -t -A -v ON_ERROR_STOP=1 -c "$1"
}

sql_f() {  # executa o SQL que chega pelo stdin
    psql -X -q -v ON_ERROR_STOP=1 -f -
}

lista_sql() {  # 'a','b','c' -- para o IN (...) do catalogo
    local out="" t
    for t in "${TAB_ARR[@]}"; do
        out+="${out:+,}'$t'"
    done
    printf '%s' "$out"
}

servico_ativo() {
    sudo -n /usr/bin/systemctl is-active --quiet "$SERVICO.service" 2>/dev/null
}

parar_servico() {
    if servico_ativo; then
        emit_inicio servico "parar" "parando $SERVICO.service"
        local ini; ini=$(agora)
        sudo -n /usr/bin/systemctl stop "$SERVICO.service"
        emit_etapa servico "parar" "parado" 0 $(( $(agora) - ini ))
    else
        log "servico $SERVICO.service ja estava parado"
    fi
}

religar_servico() {
    emit_inicio servico "religar" "religando $SERVICO.service"
    local ini; ini=$(agora)
    sudo -n /usr/bin/systemctl start "$SERVICO.service"
    sleep 2
    if servico_ativo; then
        emit_etapa servico "religar" "ativo" 0 $(( $(agora) - ini ))
    else
        emit_aviso "$SERVICO.service NAO subiu -- ver journalctl -u $SERVICO"
        emit_etapa servico "religar" "FALHOU ao religar" 0 $(( $(agora) - ini ))
    fi
}

contar_indices() {
    local lista; lista=$(lista_sql)
    sql "select count(*) from pg_index i
           join pg_class c on c.oid = i.indrelid
           join pg_namespace n on n.oid = c.relnamespace
          where n.nspname = 'public' and c.relname in ($lista)"
}

# ------------------------------------------------------------------ ddl

gerar_schema() {  # salva PK/unique/indices/checks de uma tabela (post-data)
    local t=$1 arq="$DDL_DIR/$ROTULO/$t.dump"
    if [[ -s "$arq" ]]; then
        return 0
    fi
    mkdir -p "$DDL_DIR/$ROTULO"
    pg_dump -Fc --schema-only --no-owner --no-privileges \
            -t "public.$t" -f "$arq.tmp" 2>>"$LOG"
    mv "$arq.tmp" "$arq"
}

aplicar_drops() {
    local lista antes depois
    lista=$(lista_sql)
    antes=$(contar_indices)

    # 1) FKs: as que saem do lote e as que apontam para ele
    sql "select format('ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I;',
                        n.nspname, c.relname, con.conname)
           from pg_constraint con
           join pg_class c on c.oid = con.conrelid
           join pg_namespace n on n.oid = c.relnamespace
          where con.contype = 'f'
            and (c.relname in ($lista)
                 or con.confrelid in (select oid from pg_class
                                       where relname in ($lista)
                                         and relnamespace = 'public'::regnamespace))" | sql_f

    # 2) PK / UNIQUE / CHECK (dropar a PK leva o indice junto)
    sql "select format('ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I;',
                        n.nspname, c.relname, con.conname)
           from pg_constraint con
           join pg_class c on c.oid = con.conrelid
           join pg_namespace n on n.oid = c.relnamespace
          where n.nspname = 'public'
            and c.relname in ($lista)
            and con.contype in ('p','u','c')" | sql_f

    # 3) indices soltos (os que nao sustentam constraint)
    sql "select format('DROP INDEX IF EXISTS %I.%I;', n.nspname, ic.relname)
           from pg_index i
           join pg_class ic on ic.oid = i.indexrelid
           join pg_class tc on tc.oid = i.indrelid
           join pg_namespace n on n.oid = ic.relnamespace
          where n.nspname = 'public'
            and tc.relname in ($lista)
            and not exists (select 1 from pg_constraint con
                             where con.conindid = i.indexrelid)" | sql_f

    depois=$(contar_indices)
    log "indices do lote: $antes -> $depois"
    INDICES_ANTES=$antes
}

fase_ddl() {
    local t ini
    for t in "${TAB_ARR[@]}"; do
        emit_inicio ddl "$t" "salvando schema (post-data)"
        ini=$(agora)
        gerar_schema "$t"
        emit_etapa ddl "$t" "schema salvo" 0 $(( $(agora) - ini ))
    done
    emit_inicio ddl "" "drop de constraints e indices"
    ini=$(agora)
    INDICES_ANTES=0
    aplicar_drops
    emit_etapa ddl "" "constraints e indices removidos ($INDICES_ANTES indices)" \
              0 $(( $(agora) - ini ))
}

# ------------------------------------------------------------------ restore

acertar_sequences() {  # o dump traz os ids; a sequence precisa alcancar o max
    local t=$1
    sql "select format('select setval(%L, coalesce((select max(%I) from public.%I), 1));',
                        pg_get_serial_sequence('public.'||c.relname, a.attname),
                        a.attname, c.relname)
           from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           join pg_attribute a on a.attrelid = c.oid and a.attnum > 0
          where n.nspname = 'public' and c.relname = '$t'
            and pg_get_serial_sequence('public.'||c.relname, a.attname) is not null" | sql_f
}

restaurar_uma() {  # roda em paralelo: 1 conexao por tabela
    local t=$1 arq="$DUMP_DIR/$ROTULO/$t.dump"
    local ini linhas
    emit_inicio restaurar "$t" "pg_restore --data-only"
    ini=$(agora)
    if [[ ! -s "$arq" ]]; then
        emit_aviso "dump ausente: $arq"
        return 1
    fi
    if ! pg_restore --data-only --no-owner --no-privileges \
                    -d "$PGDB" "$arq" >>"$LOG" 2>&1; then
        emit_aviso "pg_restore falhou em $t (ver $LOG)"
        return 1
    fi
    acertar_sequences "$t"
    linhas=$(sql "select count(*) from public.$t")
    emit_etapa restaurar "$t" "dados restaurados" "$linhas" \
              $(( $(agora) - ini ))
}

fazer_backup() {  # opcional: permite voltar ao estado anterior
    local stamp arq ini t
    local args=()
    stamp=$(date +%Y%m%d-%H%M%S)
    arq="$BACKUP_DIR/pre_${ROTULO}_$stamp.dump"
    for t in "${TAB_ARR[@]}"; do args+=(-t "public.$t"); done
    emit_inicio backup "" "pg_dump das tabelas do lote"
    ini=$(agora)
    if pg_dump -Fc -Z6 --no-owner --no-privileges "${args[@]}" -f "$arq" 2>>"$LOG"; then
        emit_etapa backup "" "$(basename "$arq") $(du -h "$arq" | cut -f1)" 0 \
                  $(( $(agora) - ini ))
    else
        emit_aviso "backup de rollback falhou (seguindo sem ele)"
        rm -f "$arq"
    fi
}

fase_restaurar() {
    local t ini ativos=0 falhas=0 livre necessario
    if [[ $BACKUP == 1 ]]; then
        livre=$(df -BG --output=avail "$BACKUP_DIR" | tail -1 | tr -dc '0-9')
        necessario=$(sql "select coalesce(sum(pg_total_relation_size(oid)),0)
                            from pg_class
                           where relnamespace = 'public'::regnamespace
                             and relname in ($(lista_sql))")
        # arredonda para GB e soma 2 GB de folga (indice temporario do COPY)
        necessario=$(( (${necessario:-0} + 1073741823) / 1073741824 + 2 ))
        log "backup: precisa ~${necessario} GB (com folga), livre ${livre} GB em $BACKUP_DIR"
        if (( livre > necessario )); then
            fazer_backup
        else
            emit_aviso "espaco insuficiente para backup (${livre} GB livre) -- seguindo sem"
        fi
    fi

    if [[ $PARAR == 1 ]]; then parar_servico; fi

    for t in "${TAB_ARR[@]}"; do
        emit_inicio truncar "$t" "TRUNCATE RESTART IDENTITY"
        ini=$(agora)
        if ! sql "TRUNCATE TABLE public.$t RESTART IDENTITY" >>"$LOG" 2>&1; then
            emit_aviso "TRUNCATE de $t falhou (FK de outra tabela?)"
            falhas=$((falhas + 1))
            continue
        fi
        emit_etapa truncar "$t" "" 0 $(( $(agora) - ini ))
    done

    emit_inicio restaurar "" "restore em paralelo (${JOBS} jobs)"
    for t in "${TAB_ARR[@]}"; do
        restaurar_uma "$t" &
        ativos=$((ativos + 1))
        if (( ativos >= JOBS )); then
            if ! wait -n; then falhas=$((falhas + 1)); fi
            ativos=$((ativos - 1))
        fi
    done
    while (( ativos > 0 )); do
        if ! wait -n; then falhas=$((falhas + 1)); fi
        ativos=$((ativos - 1))
    done

    if (( falhas > 0 )); then
        emit_aviso "$falhas tabela(s) com problema na fase restaurar"
        return 1
    fi
}

# ------------------------------------------------------------------ indices

recriar_uma() {  # post-data: PK, unique, indices e checks + ANALYZE
    local t=$1 arq="$DDL_DIR/$ROTULO/$t.dump"
    local ini
    emit_inicio indice "$t" "post-data (PK/unique/indices/checks)"
    ini=$(agora)
    if [[ ! -s "$arq" ]]; then
        emit_aviso "schema ausente: $arq"
        return 1
    fi
    PGOPTIONS="-c maintenance_work_mem=$MWM" \
        pg_restore --section=post-data --no-owner --no-privileges \
                   -d "$PGDB" "$arq" >>"$LOG" 2>&1 \
        || emit_aviso "post-data de $t teve erro (objeto ja existia?) -- ver $LOG"
    emit_etapa indice "$t" "indices e constraints recriados" 0 \
              $(( $(agora) - ini ))

    emit_inicio analyze "$t" "ANALYZE"
    ini=$(agora)
    sql "ANALYZE public.$t" >>"$LOG" 2>&1 || emit_aviso "ANALYZE de $t falhou"
    emit_etapa analyze "$t" "" 0 $(( $(agora) - ini ))
}

fase_indices() {
    local t ativos=0 falhas=0
    log "recriando indices em ${JOBS} jobs"
    for t in "${TAB_ARR[@]}"; do
        recriar_uma "$t" &
        ativos=$((ativos + 1))
        if (( ativos >= JOBS )); then
            if ! wait -n; then falhas=$((falhas + 1)); fi
            ativos=$((ativos - 1))
        fi
    done
    while (( ativos > 0 )); do
        if ! wait -n; then falhas=$((falhas + 1)); fi
        ativos=$((ativos - 1))
    done
    if (( falhas > 0 )); then
        emit_aviso "$falhas tabela(s) com problema na fase indices"
    fi
    local total_agora; total_agora=$(contar_indices)
    log "indices do lote agora: $total_agora"
}

# ------------------------------------------------------------------ finalizar

fase_finalizar() {
    local ini existe
    # a procedure faz TRUNCATE em c_estatisticas_totais (lock exclusivo): com o
    # site no ar os workers ficam pendurados esperando, entao para antes.
    if [[ $PARAR == 1 ]]; then parar_servico; fi
    emit_inicio estatisticas "" "sp_recalcular_estatisticas_totais"
    ini=$(agora)
    existe=$(sql "select count(*) from pg_proc
                   where proname = 'sp_recalcular_estatisticas_totais'")
    if [[ "${existe:-0}" -gt 0 ]]; then
        # e PROCEDURE: CALL (nao SELECT)
        if sql "CALL sp_recalcular_estatisticas_totais()" >>"$LOG" 2>&1; then
            emit_etapa estatisticas "" "estatisticas recalculadas" 0 \
                      $(( $(agora) - ini ))
        else
            emit_aviso "sp_recalcular_estatisticas_totais falhou -- ver $LOG"
        fi
    else
        emit_aviso "funcao sp_recalcular_estatisticas_totais nao existe nesta base"
    fi
    if [[ $PARAR == 1 ]]; then religar_servico; fi
}

# ------------------------------------------------------------------ status

fase_status() {
    local t linhas indices
    for t in "${TAB_ARR[@]}"; do
        linhas=$(sql "select count(*) from public.$t" 2>/dev/null || echo 0)
        indices=$(sql "select count(*) from pg_index i
                         join pg_class c on c.oid = i.indrelid
                        where c.relname = '$t'" 2>/dev/null || echo 0)
        printf '##STATUS|%s|%s|%s\n' "$t" "$linhas" "$indices"
    done
    if servico_ativo; then
        printf '##STATUS|servico|ativo|0\n'
    else
        printf '##STATUS|servico|parado|0\n'
    fi
}

# ------------------------------------------------------------------ main

if [[ ${#TAB_ARR[@]} -eq 0 && "$FASE" != "finalizar" ]]; then
    echo "--tabelas e obrigatorio nesta fase" >&2
    exit 2
fi

log "fase=$FASE rotulo=$ROTULO tabelas=${TABELAS:-todas} jobs=$JOBS parar=$PARAR backup=$BACKUP"
log "banco=$PGUSER@$PGHOST:$PGPORT/$PGDB base=$BASE"

case "$FASE" in
    ddl)        fase_ddl ;;
    restaurar)  fase_ddl; fase_restaurar ;;
    indices)    fase_indices ;;
    finalizar)  fase_finalizar ;;
    tudo)       fase_ddl; fase_restaurar; fase_indices; fase_finalizar ;;
    status)     fase_status ;;
    *)          echo "fase invalida: $FASE" >&2; exit 2 ;;
esac

log "fase $FASE concluida"



