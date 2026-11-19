#!/usr/bin/env bash
# Publica um lote de dumps na bancocnpj da VPS em sequencia:
#
#     enviar  ->  restaurar  ->  indices (post-data + ANALYZE + finalizar)
#
# Um log unico em logs/execucoes/<carimbo>_publicacao_<rotulo>.log registra
# passos, tempos e as tabelas migradas (o comando grava Carga/EtapaCarga e o
# script anexa um resumo por tabela no fim). Cada fase tem retomada: o envio
# de 7 GB em link lento cai no meio, e o rsync continua de onde parou.
#
#   scripts/publica_lote.sh [rotulo] [tabelas]
#
# Sem argumentos: lote 2026-11 com as 14 tabelas (10 da RFB + 4 derivadas).
# Rode sempre com setsid/nohup. O lote de 14 tabelas (7,05 GB / 231,2 M linhas)
# levou 1h28 em 18/11/2026: enviar 2 min, restaurar 24 min, indices 55 min (o
# indice de c_estabelecimentos e 54 min disso) e estatisticas 7 min. Tempos por
# tabela no README ("Publicar na VPS").
#
#   setsid nohup scripts/publica_lote.sh >/dev/null 2>&1 &
#
# Variaveis: JOBS (restore/indices em paralelo, default 4), TENTATIVAS
# (default 6), ESPERA (segundos entre tentativas, default 120).
set -uo pipefail

RAIZ=/home/neto/ProjetoCargaDados
cd "$RAIZ" || exit 1
PY=$RAIZ/venv/bin/python
ROTULO=${1:-2026-11}
TABELAS=${2:-c_cnaes,c_motivos,c_municipios,c_naturezas,c_paises,c_qualificacoes,c_socios,c_simples,c_empresas,c_estabelecimentos,c_ceps,c_ceps_estab,c_ceps_estab_sit,c_municipios_geo}
JOBS=${JOBS:-4}
TENTATIVAS=${TENTATIVAS:-6}
ESPERA=${ESPERA:-120}

LOG_DIR=$RAIZ/logs/execucoes
mkdir -p "$LOG_DIR"
LOG=$LOG_DIR/$(date +%Y%m%d-%H%M%S)_publicacao_${ROTULO}.log
INICIO=$(date +%s)
SINAL=$LOG_DIR/.publicando_${ROTULO}

registrar() {
    printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$LOG"
}

log_limpo() {
    # saida do comando: vai para o log e para o terminal, sem carimbo
    tee -a "$LOG"
}

SSH_ALVO=$("$PY" -c 'import sys;sys.path.insert(0,".");import os,django;'\
'os.environ.setdefault("DJANGO_SETTINGS_MODULE","carga_dados.settings");'\
'django.setup();from django.conf import settings;'\
'print(settings.PUBLICACAO_SSH)' 2>/dev/null)

TOTAL_LOCAL=$(du -sb "$("$PY" -c 'import sys;sys.path.insert(0,".");import os,django;'\
'os.environ.setdefault("DJANGO_SETTINGS_MODULE","carga_dados.settings");'\
'django.setup();from django.conf import settings;'\
'print(settings.PUBLICACAO_DUMP_DIR)' 2>/dev/null)/${ROTULO}" 2>/dev/null | cut -f1)
TOTAL_LOCAL=${TOTAL_LOCAL:-0}

# vigia: a cada 5 min anota quanto ja chegou na VPS (o rsync fica calado por
# horas no arquivo grande; sem isso o log pareceria travado)
vigiar() {
    while [ -f "$SINAL" ]; do
        remoto=$(ssh -o BatchMode=yes -o ConnectTimeout=20 "$SSH_ALVO" \
                 "du -sb ~/carga/dumps/$ROTULO 2>/dev/null | cut -f1" 2>/dev/null)
        remoto=${remoto:-0}
        registrar "$(awk -v r="$remoto" -v t="$TOTAL_LOCAL" 'BEGIN{
            printf "vigia: VPS %.2f GB de %.2f GB (%.1f%%)",
            r/1073741824, t/1073741824, (t>0 ? r*100/t : 0)}')"
        sleep 300
    done
}

roda_fase() {
    fase=$1
    shift
    n=1
    codigo=1
    while [ "$n" -le "$TENTATIVAS" ]; do
        registrar "---- fase $fase (tentativa $n/$TENTATIVAS) ----"
        "$PY" -u manage.py publicar_base_cnpj --fase "$fase" --rotulo "$ROTULO" \
            --tabelas "$TABELAS" --jobs "$JOBS" "$@" 2>&1 | log_limpo
        codigo=${PIPESTATUS[0]}
        if [ "$codigo" -eq 0 ]; then
            registrar "fase $fase concluida ($(duracao $(( $(date +%s) - INICIO ))))"
            return 0
        fi
        registrar "fase $fase falhou (codigo $codigo)"
        if [ "$n" -lt "$TENTATIVAS" ]; then
            registrar "nova tentativa em ${ESPERA}s"
            sleep "$ESPERA"
        fi
        n=$(( n + 1 ))
    done
    return "$codigo"
}

duracao() {
    s=${1:-0}
    printf '%dh%02dmin%02ds' $(( s / 3600 )) $(( (s % 3600) / 60 )) $(( s % 60 ))
}

resumo() {
    "$PY" - "$ROTULO" "$INICIO" "$LOG" <<'PYRESUMO'
import os
import sys
from datetime import datetime

sys.path.insert(0, '/home/neto/ProjetoCargaDados')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'carga_dados.settings')
import django

django.setup()

from django.utils import timezone

from etl.models import Carga, EtapaCarga
from etl.management.commands.publicar_base_cnpj import Command as Pub

rotulo, inicio, log = sys.argv[1], float(sys.argv[2]), sys.argv[3]
desde = timezone.make_aware(datetime.fromtimestamp(inicio))

cargas = (Carga.objects
          .filter(rotulo=rotulo, registrado_em__gte=desde,
                  observacao__contains='publicar_base_cnpj')
          .prefetch_related('etapas')
          .order_by('iniciado_em'))

linhas = []


def escreve(texto=''):
    linhas.append(texto)


def fmt_seg(valor):
    valor = valor or 0.0
    if valor < 60:
        return '%.1fs' % valor
    if valor < 3600:
        return '%dm%02ds' % (valor // 60, valor % 60)
    return '%dh%02dm' % (valor // 3600, (valor % 3600) // 60)


def fmt_num(valor):
    return f'{int(valor or 0):,}'.replace(',', '.')


escreve('')
escreve('================ RESUMO DA MIGRACAO %s ================' % rotulo)
if not cargas:
    escreve('nenhuma carga registrada nesta execucao')
else:
    for carga in cargas:
        fase = (carga.observacao or '').replace('publicar_base_cnpj --fase ', '')
        inicio_l = timezone.localtime(carga.iniciado_em).strftime('%d/%m %H:%M')
        fim_l = (timezone.localtime(carga.finalizado_em).strftime('%d/%m %H:%M')
                 if carga.finalizado_em else '-')
        escreve('carga %-4s fase %-10s %-11s %s -> %s (%s)'
                % (carga.pk, fase, carga.status, inicio_l, fim_l,
                   fmt_seg(carga.duracao_s)))

    # tabela -> etapa -> segundos. As etapas sem tabela (backup, ddl, servico,
    # estatisticas) somam em 'geral' e aparecem no rodape, nao como tabela.
    dados = {}
    geral = {'linhas': 0}
    referencia = Pub.TABELAS_ORDEM + Pub.TABELAS_DERIVADAS
    for carga in cargas:
        for etapa in carga.etapas.all():
            item = (dados.setdefault(etapa.tabela, {'linhas': 0})
                    if etapa.tabela in referencia else geral)
            item[etapa.etapa] = item.get(etapa.etapa, 0.0) + (etapa.duracao_s or 0.0)
            if etapa.quantidade:
                item['linhas'] = max(item.get('linhas', 0), etapa.quantidade)

    ordem = sorted(dados, key=lambda t: (referencia.index(t)
                                         if t in referencia else len(referencia), t))

    colunas = [('exportar', 'exportar'), ('enviar', 'enviar'),
               ('backup', 'backup'), ('restaurar', 'restore'),
               ('indice', 'indices'), ('analyze', 'analyze'),
               ('estatisticas', 'estatist'), ('servico', 'servico')]
    escreve('')
    escreve('%-24s %12s %9s %9s %9s %9s %8s %8s'
            % ('tabela', 'linhas', *[c[1] for c in colunas[:6]]))
    escreve('-' * 92)
    totais = {chave: 0.0 for chave, _ in colunas}
    for alvo in ordem + ['(geral)']:
        item = dados.get(alvo, geral) if alvo != '(geral)' else geral
        for chave, _ in colunas:
            totais[chave] += item.get(chave, 0.0)
        escreve('%-24s %12s %9s %9s %9s %9s %8s %8s' % (
            alvo, fmt_num(item.get('linhas', 0)),
            *[fmt_seg(item.get(c[0], 0.0)) for c in colunas[:6]]))
    escreve('-' * 92)
    escreve('%-24s %12s %9s %9s %9s %9s %8s %8s' % (
        'TOTAL', '', *[fmt_seg(totais[c[0]]) for c in colunas[:6]]))
    escreve('')
    escreve('estatisticas da base: %s | servico do site: %s'
            % (fmt_seg(totais['estatisticas']), fmt_seg(totais['servico'])))
    # Uma etapa 'restaurar' de tabela pequena grava duracao_s = 0.0, que e falsy:
    # testar a chave (e nao o valor) evita o resumo dizer "7 migradas" quando as
    # 14 foram truncadas e restauradas (visto no lote 2026-11 de 18/11/2026).
    migradas = [a for a in ordem if 'restaurar' in dados[a]]
    escreve('tabelas migradas: %d (%s)' % (len(migradas), ', '.join(migradas)))
escreve('=======================================================')

texto = '\n'.join(linhas)
with open(log, 'a') as arquivo:
    arquivo.write(texto + '\n')
print(texto)
PYRESUMO
}

registrar "==== migracao $ROTULO -> VPS ===="
registrar "tabelas: $TABELAS"
registrar "jobs: $JOBS | tentativas por fase: $TENTATIVAS"
registrar "log: $LOG"

trap 'rm -f "$SINAL"' EXIT INT TERM
touch "$SINAL"
( vigiar ) &
VIGIA=$!

if ! roda_fase enviar; then
    registrar "ABORTADO: envio nao terminou depois de $TENTATIVAS tentativas"
    rm -f "$SINAL"
    kill "$VIGIA" 2>/dev/null
    resumo
    exit 2
fi

if ! roda_fase restaurar --confirmar PUBLICAR; then
    registrar "ABORTADO: restore falhou -- site continua parado, ver $LOG"
    rm -f "$SINAL"
    kill "$VIGIA" 2>/dev/null
    resumo
    exit 3
fi

if ! roda_fase indices --confirmar PUBLICAR; then
    registrar "ABORTADO: indices/finalizar falhou -- religue o site na VPS"
    rm -f "$SINAL"
    kill "$VIGIA" 2>/dev/null
    resumo
    exit 4
fi

rm -f "$SINAL"
kill "$VIGIA" 2>/dev/null
wait "$VIGIA" 2>/dev/null

resumo
registrar "migracao $ROTULO concluida em $(duracao $(( $(date +%s) - INICIO )))"
