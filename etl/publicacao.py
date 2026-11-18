"""Disparo da publicacao na VPS pelo admin, em processo separado.

Mesmo desenho de `etl/execucao.py`: o admin nao faz dump nem restore, ele
chama `manage.py publicar_base_cnpj` num subprocesso com sessao propria e a
saida em arquivo de log. O navegador pode fechar; o andamento continua e
aparece na pagina de estatisticas (o comando grava Carga/EtapaCarga).

O estado da execucao fica em logs/execucoes/publicacao.json -- separado do
ativa.json da carga local, para as duas coisas poderem rodar juntas.
"""

import json
import os
import re
import signal
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

from django.conf import settings
from django.utils import timezone

from etl import execucao

FASES = ['tudo', 'exportar', 'enviar', 'restaurar', 'indices', 'finalizar',
         'status']
FASES_COM_CONFIRMACAO = ('restaurar', 'indices', 'tudo')


def diretorio():
    return execucao.diretorio()


def _arquivo_estado():
    return diretorio() / 'publicacao.json'


def _caminho_log(rotulo):
    carimbo = timezone.localtime().strftime('%Y%m%d-%H%M%S')
    seguro = re.sub(r'[^A-Za-z0-9._-]', '-', rotulo or 'publicacao')[:40]
    return diretorio() / f'{carimbo}_publicacao_{seguro}.log'


def pasta_dumps(rotulo):
    return Path(settings.PUBLICACAO_DUMP_DIR) / (rotulo or '')


def estado():
    caminho = _arquivo_estado()
    if not caminho.exists():
        return None
    try:
        dados = json.loads(caminho.read_text())
    except (OSError, ValueError):
        return None
    dados['ativo'] = execucao._pid_vivo(dados.get('pid'))
    dados['log_linhas'] = execucao.ler_log(dados.get('log'))
    return dados


def checar():
    """Testa a VPS: ssh, pg_restore, servico, espaco, .pgpass e acesso ao banco"""
    if not getattr(settings, 'PUBLICACAO_SSH', ''):
        raise ValueError('PUBLICACAO_SSH nao definido no .env.')
    try:
        host_, porta, banco, usuario = settings.PUBLICACAO_PG.split(':', 3)
    except ValueError:
        raise ValueError('PUBLICACAO_PG deve ser host:porta:banco:usuario')

    psql = f'psql -X -q -t -A -h {host_} -p {porta} -U {usuario} -d {banco}'
    script = f"""echo "host: $(hostname)"
echo "pg_restore: $(pg_restore --version 2>&1 | head -1)"
echo "acesso: $({psql} -c 'select current_user, current_database()' 2>&1 | tail -1)"
echo "disco: $(df -h "$HOME" | tail -1 | awk '{{print $4" livre de "$2}}')"
echo "servico: $(systemctl is-active {settings.PUBLICACAO_SERVICO}.service 2>&1)"
echo "pgpass: $([ -r "$HOME/.pgpass" ] && echo ok || echo AUSENTE)"
echo "carga: $(du -sh "$HOME/{settings.PUBLICACAO_REMOTO}" 2>/dev/null | cut -f1)"
echo "sudo: $(sudo -n true 2>&1 && echo ok || echo NEGADO)"
"""
    processo = subprocess.run(
        ['ssh'] + ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=20']
        + [settings.PUBLICACAO_SSH, script],
        capture_output=True, text=True)
    linhas = [l for l in (processo.stdout or '').splitlines() if l.strip()]
    if processo.returncode != 0:
        erro = (processo.stderr or '').strip().splitlines()
        raise ValueError('SSH falhou: ' + (erro[0] if erro else 'sem detalhe'))
    return linhas


def disparar(rotulo, fase='tudo', tabelas=None, jobs=4, parar_servico=True,
             backup=True):
    """Sobe `publicar_base_cnpj` em segundo plano e devolve o estado gravado"""
    atual = estado()
    if atual and atual['ativo']:
        raise ValueError(f'Ja existe uma publicacao em execucao '
                         f'(pid {atual["pid"]}). Pare a atual antes.')

    if fase not in FASES:
        fase = 'status'
    rotulo = (rotulo or '').strip()
    if not rotulo:
        raise ValueError('Informe o rotulo do lote (ex.: 2026-11).')

    comando = [sys.executable, str(Path(settings.BASE_DIR) / 'manage.py'),
               'publicar_base_cnpj',
               '--fase', fase,
               '--rotulo', rotulo,
               '--jobs', str(int(jobs or 4))]
    if tabelas:
        comando += ['--tabelas', ','.join(tabelas)]
    if fase in FASES_COM_CONFIRMACAO:
        comando += ['--confirmar', 'PUBLICAR']
    if not parar_servico:
        comando += ['--sem-parar']
    if not backup:
        comando += ['--sem-backup']

    log = _caminho_log(rotulo)
    ambiente = dict(os.environ, PYTHONUNBUFFERED='1')
    with open(log, 'wb') as saida:
        processo = subprocess.Popen(
            comando, cwd=str(settings.BASE_DIR), stdout=saida,
            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            start_new_session=True, env=ambiente)

    dados = {
        'pid': processo.pid,
        'log': str(log),
        'fase': fase,
        'rotulo': rotulo,
        'tabelas': list(tabelas or []),
        'jobs': int(jobs or 4),
        'parar_servico': bool(parar_servico),
        'backup': bool(backup),
        'comando': ' '.join(comando),
        'iniciado_em': timezone.localtime().isoformat(timespec='seconds'),
    }
    _arquivo_estado().write_text(json.dumps(dados, indent=2, ensure_ascii=False))
    return dados


def parar(espera=10):
    dados = estado()
    if not dados or not dados.get('ativo'):
        return False
    pid = int(dados['pid'])
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return False

    limite = timezone.now().timestamp() + espera
    while timezone.now().timestamp() < limite and execucao._pid_vivo(pid):
        time.sleep(0.2)
    try:
        os.waitpid(pid, os.WNOHANG)
    except (ChildProcessError, OSError):
        pass
    return not execucao._pid_vivo(pid)


def ultimas_linhas(quantidade=40):
    dados = estado()
    return deque(execucao.ler_log((dados or {}).get('log'), quantidade), quantidade) \
        if dados else deque()
