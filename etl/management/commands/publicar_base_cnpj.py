"""Publica a base CNPJ local na `bancocnpj` da VPS (producao).

A carga local continua sendo feita por `carregar_base_cnpj`; este comando so
leva o resultado para producao, em quatro pecas:

    exportar   pg_dump -Fc de cada tabela no SSD local (um arquivo por tabela)
    enviar     rsync retomavel para ~/carga/dumps/<rotulo>/ na VPS
    restaurar  SSH: drop de constraints/indices, TRUNCATE, pg_restore --data-only
    indices    SSH: post-data (PK/unique/indices/checks) + ANALYZE + religa

Um dump por tabela (e nao um dumpao) porque assim a escolha por tabela vale em
todas as fases: refazer so c_paises custa segundos, um arquivo ruim nao
invalida o lote e o ensaio por tabela convive com a carga completa.

Tempos medidos no lote 2026-11 (14 tabelas, 7,05 GB / 231,2 M linhas, 18/11/2026,
VPS com 4 jobs e disco local SSD): enviar 1min52s (os hashes ja estavam iguais),
restaurar 24 min (backup de rollback 15,5 min + restore 9 min), indices 55 min
(so o indice de c_estabelecimentos sao 54 min) e estatisticas 7 min -- 1h28 no
total. Por tabela: c_estabelecimentos 74,4 M/8m32s/54m27s, c_empresas 71,1 M/
3m39s/20m34s, c_simples 51,3 M/2m31s/4m38s, c_socios 28,7 M/1m50s/8m38s
(linhas/restore/indice). A tabela completa esta no README ("Publicar na VPS").

Nada aqui imprime senha: o pg_dump local usa PG_* do .env e o pg_restore da
VPS usa o ~/.pgpass do usuario remoto. O acesso e sempre por SSH -- a porta
5432 da VPS e fechada para fora.
"""

import hashlib
import os
import shlex
import shutil
import subprocess
import time
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from etl import estatisticas
from etl.conexoes.postgres import get_postgres_connection
from etl.models import Carga, EtapaCarga
from etl.management.commands.carregar_base_cnpj import Command as CarregarBase

SSH_OPTS = [
    '-o', 'BatchMode=yes',
    '-o', 'ConnectTimeout=20',
    '-o', 'ServerAliveInterval=30',
    '-o', 'ServerAliveCountMax=6',
]


class Command(BaseCommand):
    help = 'Publica a base CNPJ local na VPS (dump por tabela, rsync, restore)'

    TABELAS_DOMINIO = CarregarBase.TABELAS_DOMINIO
    TABELAS_GRANDES = CarregarBase.TABELAS_GRANDES
    TABELAS_ORDEM = CarregarBase.TABELAS_ORDEM
    # Derivadas locais: acompanham a base para o site nao exibir contagem velha
    # (c_ceps = nome/coordenada do CEP, c_municipios_geo = TOM/IBGE/UF,
    # c_ceps_estab e c_ceps_estab_sit = total por CEP e a paginacao)
    TABELAS_DERIVADAS = ['c_ceps', 'c_ceps_estab', 'c_ceps_estab_sit',
                         'c_municipios_geo']

    carga = None
    avisos = []

    def add_arguments(self, parser):
        parser.add_argument('--fase', default='status',
                            choices=['exportar', 'enviar', 'restaurar',
                                     'indices', 'finalizar', 'tudo', 'status'])
        parser.add_argument('--tabelas', type=str,
                            default=','.join(self.TABELAS_ORDEM),
                            help='Tabelas do lote (default: as 10 da RFB)')
        parser.add_argument('--rotulo', type=str, default='',
                            help='Lote, ex.: 2026-11 (nome da pasta dos dumps)')
        parser.add_argument('--jobs', type=int,
                            default=getattr(settings, 'PUBLICACAO_JOBS', 4))
        parser.add_argument('--confirmar', type=str, default='',
                            help='Digite PUBLICAR nas fases restaurar/indices/tudo')
        parser.add_argument('--parar', dest='parar', action='store_true',
                            default=True,
                            help='Para o servico do site durante o restore')
        parser.add_argument('--sem-parar', dest='parar', action='store_false')
        parser.add_argument('--backup', dest='backup', action='store_true',
                            default=True,
                            help='pg_dump de rollback na VPS antes do restore')
        parser.add_argument('--sem-backup', dest='backup', action='store_false')
        parser.add_argument('--sem-religar', dest='religar',
                            action='store_false', default=True,
                            help='Nao religa o servico no fim dos indices')

    # ------------------------------------------------------------ utilidades

    def _write(self, mensagem):
        self.stdout.write(mensagem)

    def _tabelas(self, options):
        pedidas = [t.strip() for t in options['tabelas'].split(',') if t.strip()]
        validas = self.TABELAS_ORDEM + self.TABELAS_DERIVADAS
        desconhecidas = [t for t in pedidas if t not in validas]
        if desconhecidas:
            raise CommandError('Tabela(s) desconhecida(s): '
                               + ', '.join(desconhecidas))
        return [t for t in validas if t in pedidas]

    def _pasta(self, rotulo):
        pasta = Path(settings.PUBLICACAO_DUMP_DIR) / rotulo
        pasta.mkdir(parents=True, exist_ok=True)
        return pasta

    def _ambiente_local(self):
        """pg_dump/pg_restore locais sem depender do .pgpass"""
        ambiente = dict(os.environ)
        ambiente.update(
            PGHOST=str(settings.PG_HOST or ''),
            PGPORT=str(settings.PG_PORT or ''),
            PGDATABASE=str(settings.PG_DB or ''),
            PGUSER=str(settings.PG_USER or ''),
            PGPASSWORD=str(settings.PG_PASSWORD or ''),
        )
        return ambiente


    def _contar_local(self, tabela):
        try:
            conn = get_postgres_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT count(*) FROM public.{tabela}')
                    return int(cur.fetchone()[0])
            finally:
                conn.close()
        except Exception as exc:  # contagem e informativa: nao derruba a fase
            self._write(f'   aviso: nao consegui contar {tabela}: {exc}')
            return 0

    def _exigir_ssh(self):
        if not getattr(settings, 'PUBLICACAO_SSH', ''):
            raise CommandError(
                'PUBLICACAO_SSH nao definido no .env -- sem isso a publicacao '
                'nao tem para onde ir.')

    def _ssh(self, comando, entrada=None):
        """Executa um comando na VPS e devolve (codigo, saida)"""
        processo = subprocess.run(
            ['ssh'] + SSH_OPTS + [settings.PUBLICACAO_SSH, comando],
            input=entrada, capture_output=True, text=True)
        return processo.returncode, (processo.stdout or '') + (processo.stderr or '')

    def _sha256(self, caminho):
        digest = hashlib.sha256()
        with open(caminho, 'rb') as arquivo:
            for bloco in iter(lambda: arquivo.read(1024 * 1024 * 8), b''):
                digest.update(bloco)
        return digest.hexdigest()

    def _registrar(self, etapa, tabela='', detalhe='', quantidade=0,
                   duracao=0.0, texto=None):
        self._write('   ok  %s %s %s (%s linhas, %.1fs)'
                    % (etapa, tabela, texto or detalhe,
                       f'{int(quantidade):,}'.replace(',', '.'), duracao))
        estatisticas.registrar(self.carga, etapa, tabela, detalhe,
                               quantidade, duracao)

    def _abrir(self, etapa, tabela='', detalhe=''):
        estatisticas.iniciar_etapa(self.carga, etapa, tabela, detalhe)

    # ------------------------------------------------------------ exportar

    def fase_exportar(self, tabelas):
        pasta = self._pasta(self.rotulo)
        livre = shutil.disk_usage(str(pasta)).free
        if livre < 10 * 1024 ** 3:
            raise CommandError(
                'Pouco espaco em %s (%.1f GB livres); o lote completo precisa '
                'de ~8 GB.' % (pasta, livre / 1024 ** 3))

        ambiente = self._ambiente_local()
        manifesto = []
        for tabela in tabelas:
            destino = pasta / f'{tabela}.dump'
            self._write(f'-> pg_dump {tabela} -> {destino}')
            inicio = time.time()
            self._abrir(EtapaCarga.EXPORTAR, tabela, 'pg_dump -Fc')
            comando = ['pg_dump', '-Fc', '-Z6', '--no-owner',
                       '--no-privileges', '-t', f'public.{tabela}',
                       '-f', str(destino)]
            processo = subprocess.run(comando, env=ambiente,
                                      capture_output=True, text=True)
            if processo.returncode != 0:
                raise CommandError(f'pg_dump de {tabela} falhou: '
                                   f'{processo.stderr.strip()}')
            # valida o arquivo antes de gastar horas de upload
            checagem = subprocess.run(['pg_restore', '--list', str(destino)],
                                      capture_output=True, text=True)
            if checagem.returncode != 0:
                raise CommandError(f'dump de {tabela} invalido: '
                                   f'{checagem.stderr.strip()}')
            duracao = time.time() - inicio
            linhas = self._contar_local(tabela)
            digest = self._sha256(destino)
            manifesto.append({'tabela': tabela, 'arquivo': destino.name,
                              'bytes': destino.stat().st_size,
                              'linhas': linhas, 'sha256': digest})
            self._registrar(EtapaCarga.EXPORTAR, tabela, 'pg_dump -Fc',
                            linhas, duracao)

        # o manifesto acumula: exportar uma tabela por vez (link lento) nao
        # pode apagar o registro das que ja foram exportadas
        manifesto_txt = pasta / 'manifest.sha256'
        registros = {}
        if manifesto_txt.exists():
            for linha in manifesto_txt.read_text().splitlines():
                partes = linha.split()
                if len(partes) == 2:
                    registros[partes[1]] = partes[0]
        for item in manifesto:
            registros[item['arquivo']] = item['sha256']
        manifesto_txt.write_text(''.join(
            f'{digest}  {arquivo}\n' for arquivo, digest in sorted(registros.items())))
        self._write(f'manifesto: {manifesto_txt} ({len(registros)} dump(s))')

    # ------------------------------------------------------------ enviar

    def _home_remoto(self):
        """Caminho absoluto do home na VPS (o rsync nao interpreta $HOME)"""
        if not getattr(self, '_home', None):
            codigo, saida = self._ssh('echo "$HOME"')
            if codigo != 0 or not saida.strip():
                raise CommandError(f'nao consegui ler o HOME remoto: {saida}')
            self._home = saida.strip().splitlines()[-1].strip()
        return self._home

    def _base_remota(self):
        return f'{self._home_remoto()}/{settings.PUBLICACAO_REMOTO}'

    def _dir_remoto(self, sub=''):
        return f'{self._base_remota()}/{sub}'.rstrip('/')

    def _garantir_script(self):
        origem = Path(settings.BASE_DIR) / 'scripts' / 'restaura_base_cnpj.sh'
        if not origem.exists():
            raise CommandError(f'script remoto ausente: {origem}')
        alvo = self._dir_remoto('restaura_base_cnpj.sh')
        codigo, saida = self._ssh(
            f'mkdir -p {self._base_remota()} && cat > {alvo}',
            entrada=origem.read_text())
        if codigo != 0:
            raise CommandError(f'nao consegui enviar o script remoto: {saida}')
        self._ssh(f'chmod +x {alvo}')

    def _bytes_remoto(self, caminho):
        codigo, saida = self._ssh(f'stat -c%s {caminho} 2>/dev/null || echo 0')
        if codigo != 0:
            return 0
        try:
            return int(saida.strip().splitlines()[-1] or 0)
        except (ValueError, IndexError):
            return 0

    def _rsync(self, origem, destino, retomar=True):
        comando = ['rsync', '-a', '--partial']
        if retomar:
            # --append-verify so e seguro quando o arquivo da VPS e prefixo do
            # local: por isso o chamador remove o remoto quando ele e maior.
            comando.append('--append-verify')
        else:
            comando += ['--checksum', '--inplace']
        comando += [str(origem), f'{settings.PUBLICACAO_SSH}:{destino}/']
        return subprocess.run(comando, capture_output=True, text=True)

    def _sha_remoto(self, caminho):
        codigo, saida = self._ssh(
            f'sha256sum {caminho} 2>/dev/null | cut -d" " -f1')
        if codigo != 0:
            return ''
        return (saida.strip().splitlines() or [''])[-1].strip()

    def _digests_manifesto(self, pasta):
        """{arquivo: sha256} a partir do manifest.sha256 local"""
        arquivo = pasta / 'manifest.sha256'
        if not arquivo.exists():
            return {}
        mapa = {}
        for linha in arquivo.read_text().splitlines():
            partes = linha.split()
            if len(partes) == 2:
                mapa[partes[1]] = partes[0]
        return mapa

    def fase_enviar(self, tabelas):
        self._exigir_ssh()
        pasta = self._pasta(self.rotulo)
        remoto = self._dir_remoto(f'dumps/{self.rotulo}')
        codigo, saida = self._ssh(f'mkdir -p {remoto}')
        if codigo != 0:
            raise CommandError(f'nao consegui criar {remoto}: {saida}')
        self._garantir_script()
        digests = self._digests_manifesto(pasta)

        for tabela in tabelas:
            origem = pasta / f'{tabela}.dump'
            if not origem.exists():
                raise CommandError(f'exporte {tabela} antes de enviar '
                                   f'(falta {origem})')
            inicio = time.time()
            alvo = f'{remoto}/{origem.name}'
            local_bytes = origem.stat().st_size
            detalhe_envio = 'rsync %.1f MB' % (local_bytes / 1024 ** 2)
            self._abrir(EtapaCarga.ENVIAR, tabela, detalhe_envio)
            esperado = digests.get(origem.name, '')
            if esperado and self._sha_remoto(alvo) == esperado:
                self._write(f'   {tabela}: dump identico ja na VPS, sem envio')
                self._registrar(EtapaCarga.ENVIAR, tabela, detalhe_envio,
                                0, 0.0)
                continue
            # --append-verify nao trunca: se o dump da VPS nao for menor (lote
            # anterior, ou retomada de outro arquivo), o final junta os dois e
            # o sha256 quebra. Nesse caso remove e envia inteiro.
            if self._bytes_remoto(alvo) >= local_bytes:
                self._ssh(f'rm -f {alvo}')
            processo = self._rsync(origem, remoto)
            if processo.returncode != 0:
                raise CommandError(f'rsync de {tabela} falhou: '
                                   f'{processo.stderr.strip()}')
            duracao = time.time() - inicio
            self._write('   %.0f MB em %.0fs (%.2f MB/s)'
                        % (local_bytes / 1024 ** 2, duracao,
                           local_bytes / 1024 ** 2 / max(duracao, 1)))
            self._registrar(EtapaCarga.ENVIAR, tabela, detalhe_envio, 0, duracao)

        # o manifesto remoto leva so as tabelas deste lote (senao o
        # sha256sum -c acusa os dumps que ainda nao subiram)
        manifesto = pasta / 'manifest.sha256'
        if manifesto.exists():
            meu = {f'{tabela}.dump' for tabela in tabelas}
            linhas = [linha for linha in manifesto.read_text().splitlines()
                      if len(linha.split()) == 2 and linha.split()[1] in meu]
            if linhas:
                codigo, saida = self._ssh(
                    f'cat > {remoto}/manifest.sha256',
                    entrada='\n'.join(linhas) + '\n')
                if codigo != 0:
                    raise CommandError(f'manifesto nao enviado: {saida}')
                codigo, saida = self._ssh(
                    f'cd {remoto} && sha256sum -c manifest.sha256')
                if codigo != 0:
                    # retomada parcial pode ter deixado arquivo truncado: refaz
                    # o envio inteiro uma vez antes de desistir
                    self._write(f'   sha256 nao confere ({saida.strip()}); '
                                'reenviando os dumps inteiros')
                    for tabela in tabelas:
                        origem = pasta / f'{tabela}.dump'
                        self._ssh(f'rm -f {remoto}/{origem.name}')
                        processo = self._rsync(origem, remoto, retomar=False)
                        if processo.returncode != 0:
                            raise CommandError(
                                f'rsync de {tabela} falhou: '
                                f'{processo.stderr.strip()}')
                    self._ssh(f'cat > {remoto}/manifest.sha256',
                              entrada='\n'.join(linhas) + '\n')
                    codigo, saida = self._ssh(
                        f'cd {remoto} && sha256sum -c manifest.sha256')
                    if codigo != 0:
                        raise CommandError(
                            f'sha256 nao confere na VPS: {saida}')
                self._write('sha256 conferido na VPS')

    # ------------------------------------------------------------ VPS

    def _comando_remoto(self, fase, tabelas, parar=None, backup=None):
        parar = self.parar if parar is None else parar
        backup = self.backup if backup is None else backup
        try:
            host_, porta, banco, usuario = settings.PUBLICACAO_PG.split(':', 3)
        except ValueError:
            raise CommandError('PUBLICACAO_PG deve ser host:porta:banco:usuario')
        partes = [
            'bash', self._base_remota() + '/restaura_base_cnpj.sh',
            '--fase', fase,
            '--rotulo', shlex.quote(self.rotulo),
            '--tabelas', shlex.quote(','.join(tabelas)),
            '--jobs', str(self.jobs),
            '--servico', shlex.quote(str(settings.PUBLICACAO_SERVICO)),
            '--base', self._base_remota(),
            '--host-pg', shlex.quote(host_),
            '--porta', shlex.quote(porta),
            '--banco', shlex.quote(banco),
            '--usuario', shlex.quote(usuario),
        ]
        partes.append('--parar' if parar else '--sem-parar')
        partes.append('--backup' if backup else '--sem-backup')
        return ' '.join(partes)

    def _tratar(self, linha):
        """Converte a saida do script remoto em etapas da Carga"""
        linha = linha.rstrip('\n')
        if not linha.strip():
            return
        if linha.startswith('##INICIO|'):
            partes = (linha.split('|', 3) + ['', '', ''])[:4]
            _, etapa, tabela, detalhe = partes
            self._abertas[(etapa, tabela)] = detalhe
            self._abrir(etapa, tabela, detalhe)
            self._write(f'   .. {etapa} {tabela} {detalhe}'.rstrip())
            return
        if linha.startswith('##ETAPA|'):
            partes = (linha.split('|') + [''] * 6)[:6]
            _, etapa, tabela, detalhe, quantidade, duracao = partes
            # a chave da etapa e a do ##INICIO (senao duplica a linha); o que
            # aparece na tela e o detalhe final, que as vezes traz o arquivo
            chave = self._abertas.pop((etapa, tabela), '') or detalhe
            try:
                quantidade = int(float(quantidade or 0))
            except ValueError:
                quantidade = 0
            try:
                duracao = float(duracao or 0)
            except ValueError:
                duracao = 0.0
            self._registrar(etapa, tabela, chave, quantidade, duracao,
                            texto=detalhe or chave)
            return
        if linha.startswith('##AVISO|'):
            texto = linha.split('|', 1)[1]
            self.avisos.append(texto)
            self._write(f'   AVISO: {texto}')
            return
        if linha.startswith('##STATUS|'):
            self.status_remoto.append(linha.split('|')[1:])
            return
        self._write(f'   {linha}')

    def _executar_remoto(self, comando):
        self._write(f'   $ ssh {settings.PUBLICACAO_SSH} (fase remota)')
        processo = subprocess.Popen(
            ['ssh'] + SSH_OPTS + [settings.PUBLICACAO_SSH, comando],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, text=True, bufsize=1)
        for linha in processo.stdout:
            self._tratar(linha)
        codigo = processo.wait()
        if codigo != 0:
            raise CommandError(f'fase remota terminou com codigo {codigo} '
                               '(veja o log acima e ~/carga/logs na VPS)')

    def fase_remota(self, fase, tabelas, parar=None, backup=None):
        self._exigir_ssh()
        self._garantir_script()
        self._executar_remoto(
            self._comando_remoto(fase, tabelas, parar=parar, backup=backup))

    def fase_indices(self, tabelas):
        self.fase_remota('indices', tabelas)
        # religar o site e recalcular estatisticas: nao mexe em dados
        self.fase_remota('finalizar', [], parar=self.religar, backup=False)

    # ------------------------------------------------------------ status

    def fase_status(self, tabelas):
        raiz = Path(settings.PUBLICACAO_DUMP_DIR)
        if not self.rotulo:
            self._write(f'Rotulos em {raiz}:')
            for pasta in sorted(raiz.glob('*')):
                if not pasta.is_dir():
                    continue
                dumps = list(pasta.glob('*.dump'))
                total = sum(p.stat().st_size for p in dumps)
                self._write('   %-12s %2d dump(s) %7.2f GB'
                            % (pasta.name, len(dumps), total / 1024 ** 3))
        else:
            pasta = raiz / self.rotulo
            self._write(f'Dumps locais em {pasta}:')
            for tabela in tabelas:
                arquivo = pasta / f'{tabela}.dump'
                if arquivo.exists():
                    self._write('   %-22s %7.2f GB'
                                % (tabela, arquivo.stat().st_size / 1024 ** 3))
                else:
                    self._write(f'   {tabela:<22} (nao exportado)')
        self._exigir_ssh()
        self._garantir_script()
        self.status_remoto = []
        self._executar_remoto(self._comando_remoto('status', tabelas,
                                                   backup=False))
        self._write('Na VPS (tabela | linhas | indices):')
        for item in self.status_remoto:
            if item and item[0] == 'servico':
                self._write('   servico %s' % (item[1] if len(item) > 1 else ''))
                continue
            self._write('   ' + ' | '.join(item))

    # ------------------------------------------------------------ handle

    def handle(self, *args, **options):
        self.rotulo = (options['rotulo'] or '').strip()
        if not self.rotulo and options['fase'] != 'status':
            raise CommandError('--rotulo e obrigatorio (ex.: 2026-11)')
        self.jobs = max(1, int(options['jobs'] or 1))
        self.parar = bool(options['parar'])
        self.backup = bool(options['backup'])
        self.religar = bool(options['religar'])
        self._abertas = {}
        self.avisos = []
        self.status_remoto = []
        self.carga = None

        tabelas = self._tabelas(options)
        fase = options['fase']
        if fase in ('restaurar', 'indices', 'tudo'):
            if (options['confirmar'] or '').strip().upper() != 'PUBLICAR':
                raise CommandError(
                    'Estas fases mexem na base de producao: repita com '
                    '--confirmar PUBLICAR')

        self._write('=' * 60)
        self._write(f'Publicacao {self.rotulo or "(todos)"} -> '
                    f'{settings.PUBLICACAO_SSH or "(PUBLICACAO_SSH vazio)"}')
        self._write(f'   fase: {fase}')
        self._write(f'   tabelas: {", ".join(tabelas) or "(nenhuma)"}')
        self._write('=' * 60)

        if fase != 'status':
            self.carga = estatisticas.iniciar_carga(
                self.rotulo, str(self._pasta(self.rotulo)),
                observacao=f'publicar_base_cnpj --fase {fase}',
                host=f'producao:{settings.PUBLICACAO_SSH or "-"}'[:80])

        try:
            if fase == 'exportar':
                self.fase_exportar(tabelas)
            elif fase == 'enviar':
                self.fase_enviar(tabelas)
            elif fase == 'restaurar':
                self.fase_remota('restaurar', tabelas)
            elif fase == 'indices':
                self.fase_indices(tabelas)
            elif fase == 'finalizar':
                self.fase_remota('finalizar', [], parar=self.religar,
                                 backup=False)
            elif fase == 'tudo':
                self.fase_exportar(tabelas)
                self.fase_enviar(tabelas)
                self.fase_remota('restaurar', tabelas)
                self.fase_indices(tabelas)
            else:
                self.fase_status(tabelas)
        except (Exception, KeyboardInterrupt):
            estatisticas.concluir_carga(self.carga, Carga.STATUS_FALHOU)
            raise

        estatisticas.concluir_carga(self.carga)
        self._write('')
        if self.avisos:
            self._write(f'{len(self.avisos)} aviso(s) -- confira o log acima')
        self._write('publicacao concluida')



