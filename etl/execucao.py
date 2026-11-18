"""Disparo da carga pelo admin, em processo separado.

O admin nao carrega nada: ele chama `manage.py carregar_base_cnpj` num
subprocesso com sessao propria e a saida em arquivo de log. Assim a carga
nao depende da aba do navegador e continua rodando mesmo que o runserver
recarregue. O andamento aparece na pagina de estatisticas, que le a Carga
gravada pelo proprio comando.

O estado da execucao (pid, log, parametros) fica em logs/execucoes/ativa.json.
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

FASES = ['tudo', 'ddl', 'truncar', 'carregar', 'indices']


# ------------------------------------------------------------------ caminhos

def diretorio():
    caminho = Path(settings.CARGA_LOG_DIR)
    caminho.mkdir(parents=True, exist_ok=True)
    return caminho


def _arquivo_estado():
    return diretorio() / 'ativa.json'


def _caminho_log(rotulo):
    carimbo = timezone.localtime().strftime('%Y%m%d-%H%M%S')
    seguro = re.sub(r'[^A-Za-z0-9._-]', '-', rotulo or 'carga')[:40]
    return diretorio() / f'{carimbo}_{seguro}.log'


# ------------------------------------------------------------------ pastas

def raizes():
    """Pastas onde o admin aceita procurar os arquivos da RFB"""
    return [Path(p).expanduser() for p in settings.CARGA_RAIZES if p]


def validar_pasta(caminho):
    """Resolve a pasta e confere se esta dentro de uma raiz permitida"""
    pasta = Path(caminho or '').expanduser().resolve()
    if not pasta.is_dir():
        raise ValueError(f'Pasta nao encontrada: {pasta}')
    for raiz in raizes():
        try:
            pasta.relative_to(raiz.expanduser().resolve())
        except ValueError:
            continue
        return pasta
    permitidas = ', '.join(str(r) for r in raizes())
    raise ValueError(f'Pasta fora das raizes permitidas ({permitidas}): {pasta}')


def listar_pastas():
    """Pastas com .zip/.parquet, para o campo de escolha do formulario"""
    pastas = []
    for raiz in raizes():
        if not raiz.is_dir():
            continue
        candidatas = [raiz] + sorted(p for p in raiz.iterdir() if p.is_dir())
        for pasta in candidatas:
            try:
                tem = (next(pasta.glob('*.zip'), None) or
                       next(pasta.glob('*.parquet'), None))
            except OSError:
                continue
            if tem:
                pastas.append(str(pasta))
    return pastas


def contar_arquivos(pasta):
    """Arquivos .zip/.parquet por tabela na pasta (o que a carga vai ler)"""
    from etl.processadores.cnpj_processor import PADROES_ARQUIVO_TABELA

    contagem = {}
    try:
        nomes = [p.name for p in Path(pasta).iterdir()
                 if p.suffix.lower() in ('.zip', '.parquet')]
    except OSError:
        return contagem
    for nome in sorted(nomes):
        for padrao, alvo in PADROES_ARQUIVO_TABELA.items():
            if re.match(padrao, nome, re.IGNORECASE):
                contagem[alvo] = contagem.get(alvo, 0) + 1
                break
    return contagem


# ------------------------------------------------------------------ estado

def _pid_zumbi(pid):
    """Processo terminou mas o pai ainda nao recolheu (kill 0 diria 'vivo')"""
    try:
        with open(f'/proc/{pid}/stat', 'r') as arquivo:
            return arquivo.read().rsplit(')', 1)[-1].split()[0] == 'Z'
    except (OSError, IndexError):
        return False


def _pid_vivo(pid):
    try:
        os.kill(int(pid), 0)
    except (TypeError, ValueError):
        return False
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return not _pid_zumbi(pid)


def ler_log(caminho, linhas=40):
    """Ultimas linhas do log (o arquivo inteiro pode ter milhares)"""
    if not caminho:
        return []
    try:
        with open(caminho, 'r', errors='replace') as arquivo:
            return [linha.rstrip('\n') for linha in deque(arquivo, maxlen=linhas)]
    except OSError:
        return []


def estado():
    """Execucao disparada pelo admin: dict com pid/log/ativo, ou None"""
    caminho = _arquivo_estado()
    if not caminho.exists():
        return None
    try:
        dados = json.loads(caminho.read_text())
    except (OSError, ValueError):
        return None
    dados['ativo'] = _pid_vivo(dados.get('pid'))
    dados['log_linhas'] = ler_log(dados.get('log'))
    return dados


# ------------------------------------------------------------------ disparo

def disparar(pasta, fase='tudo', tabelas=None, rotulo='', batch=50000,
             workers=4, limit=0):
    """Sobe `carregar_base_cnpj` em segundo plano e devolve o estado gravado"""
    atual = estado()
    if atual and atual['ativo']:
        raise ValueError(f'Ja existe uma carga em execucao (pid {atual["pid"]}). '
                         'Pare a atual antes de disparar outra.')

    pasta = validar_pasta(pasta)
    fase = fase if fase in FASES else 'tudo'
    rotulo = (rotulo or pasta.name or 'carga').strip()[:60]

    comando = [sys.executable, str(Path(settings.BASE_DIR) / 'manage.py'),
               'carregar_base_cnpj',
               '--pasta', str(pasta),
               '--fase', fase,
               '--rotulo', rotulo,
               '--batch', str(int(batch)),
               '--workers', str(int(workers))]
    if tabelas:
        comando += ['--tabelas', ','.join(tabelas)]
    if limit:
        comando += ['--limit', str(int(limit))]

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
        'pasta': str(pasta),
        'fase': fase,
        'rotulo': rotulo,
        'tabelas': list(tabelas or []),
        'limit': int(limit or 0),
        'comando': ' '.join(comando),
        'iniciado_em': timezone.localtime().isoformat(timespec='seconds'),
    }
    _arquivo_estado().write_text(json.dumps(dados, indent=2, ensure_ascii=False))
    return dados


def parar(espera=10):
    """SIGTERM no grupo do processo; o comando fecha a carga antes de sair"""
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
    while timezone.now().timestamp() < limite and _pid_vivo(pid):
        time.sleep(0.2)
    try:
        os.waitpid(pid, os.WNOHANG)
    except (ChildProcessError, OSError):
        pass  # processo de outro worker/worker do servidor: nao e nosso filho
    return not _pid_vivo(pid)
