"""Estatisticas das cargas da base CNPJ (Postgres django_internal, so admin).

O dado pesado continua no PostgreSQL (bancocnpj). Aqui ficam apenas os tempos
e as quantidades de cada execucao, para a pagina
/admin/etl/carga/estatisticas/ mostrar o historico acumulado por periodo.

    Carga       -> uma linha por execucao (lote da RFB, inicio/fim, status)
    EtapaCarga  -> uma linha por etapa/tabela/arquivo/indice daquela execucao
"""

from django.db import models
from django.utils import timezone


class ArquivoProcessado(models.Model):
    """Tabela legada da migracao 0001.

    O controle real de arquivos ja processados vive no PostgreSQL (bancocnpj),
    via ControleArquivos (psycopg2). Este modelo fica declarado apenas para o
    migrate nao derrubar a tabela antiga; no django_internal ele existe vazio e
    nao e usado. Nao registrar no admin.
    """

    nome_arquivo = models.CharField(max_length=500, unique=True)
    caminho_completo = models.TextField()
    data_processamento = models.DateTimeField(auto_now_add=True)
    registros_importados = models.IntegerField()
    banco_destino = models.CharField(max_length=20)
    status = models.CharField(default='SUCESSO', max_length=20)
    tabela = models.CharField(max_length=50)

    class Meta:
        ordering = ['-data_processamento']
        verbose_name = 'arquivo processado (legado)'
        verbose_name_plural = 'arquivos processados (legado)'



class Carga(models.Model):
    STATUS_EM_ANDAMENTO = 'em_andamento'
    STATUS_CONCLUIDA = 'concluida'
    STATUS_FALHOU = 'falhou'
    STATUS_CHOICES = [
        (STATUS_EM_ANDAMENTO, 'Em andamento'),
        (STATUS_CONCLUIDA, 'Concluida'),
        (STATUS_FALHOU, 'Falhou'),
    ]

    rotulo = models.CharField(max_length=60, db_index=True,
                              help_text='Lote da RFB, ex.: 2026-11')
    pasta = models.CharField(max_length=255, blank=True,
                             help_text='Pasta de onde os arquivos foram lidos')
    host = models.CharField(max_length=80, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES,
                              default=STATUS_EM_ANDAMENTO, db_index=True)
    iniciado_em = models.DateTimeField(default=timezone.now, db_index=True)
    finalizado_em = models.DateTimeField(null=True, blank=True)
    observacao = models.TextField(blank=True)
    registrado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'carga'
        verbose_name_plural = 'cargas'
        ordering = ['-iniciado_em']

    def __str__(self):
        local = timezone.localtime(self.iniciado_em)
        return f'{self.rotulo} - {local:%d/%m/%Y %H:%M}'

    # ------------------------------------------------------------ derivados

    def _etapas(self):
        """Lista as etapas (usa o cache do prefetch_related quando existe)"""
        return list(self.etapas.all())

    @property
    def duracao_s(self):
        fim = self.finalizado_em or timezone.now()
        return max((fim - self.iniciado_em).total_seconds(), 0.0)

    def soma_etapas(self, *etapas):
        return sum(e.duracao_s for e in self._etapas() if e.etapa in etapas)

    @property
    def tempo_carga_s(self):
        # RESTAURAR e a etapa equivalente a CARREGAR quando a carga vem de um
        # dump (publicacao na VPS): as duas entram no mesmo tempo.
        return self.soma_etapas(EtapaCarga.CARREGAR, EtapaCarga.RESTAURAR)

    @property
    def tempo_indices_s(self):
        return self.soma_etapas(EtapaCarga.INDICE, EtapaCarga.ANALYZE)

    @property
    def tempo_contadores_s(self):
        return self.soma_etapas(EtapaCarga.CONTADOR)

    @property
    def linhas_carregadas(self):
        return sum(e.quantidade for e in self._etapas()
                   if e.etapa in (EtapaCarga.CARREGAR, EtapaCarga.RESTAURAR))

    @property
    def linhas_por_segundo(self):
        tempo = self.tempo_carga_s
        if not tempo:
            return None
        return self.linhas_carregadas / tempo

    @property
    def etapas_abertas(self):
        """Etapas ainda rodando (a pagina mostra o progresso em tempo real)"""
        return [e for e in self._etapas() if e.em_andamento]


class EtapaCarga(models.Model):
    DDL = 'ddl'
    TRUNCAR = 'truncar'
    CARREGAR = 'carregar'
    ARQUIVO = 'arquivo'
    INDICE = 'indice'
    ANALYZE = 'analyze'
    CONTADOR = 'contador'
    # Publicacao na VPS (dump por tabela -> rsync -> pg_restore)
    EXPORTAR = 'exportar'
    ENVIAR = 'enviar'
    RESTAURAR = 'restaurar'
    BACKUP = 'backup'
    SERVICO = 'servico'
    ESTATISTICAS = 'estatisticas'
    ETAPA_CHOICES = [
        (DDL, 'DDL (drop de indices)'),
        (TRUNCAR, 'TRUNCATE'),
        (CARREGAR, 'Carga por tabela'),
        (ARQUIVO, 'Carga por arquivo'),
        (INDICE, 'Indice / constraint'),
        (ANALYZE, 'ANALYZE'),
        (CONTADOR, 'Contador por CEP'),
        (EXPORTAR, 'Dump (pg_dump)'),
        (ENVIAR, 'Envio (rsync)'),
        (RESTAURAR, 'Restore por tabela'),
        (BACKUP, 'Backup de rollback'),
        (SERVICO, 'Servico do site'),
        (ESTATISTICAS, 'Estatisticas da base'),
    ]


    # A etapa nasce em andamento (iniciar_etapa) e e fechada pelo registrar.
    # Sem isso, um indice de 40 min so aparecia na pagina depois de terminar.
    SITUACAO_ANDAMENTO = 'em_andamento'
    SITUACAO_CONCLUIDA = 'concluida'
    SITUACAO_CHOICES = [
        (SITUACAO_ANDAMENTO, 'Em andamento'),
        (SITUACAO_CONCLUIDA, 'Concluida'),
    ]

    carga = models.ForeignKey(Carga, on_delete=models.CASCADE,
                              related_name='etapas')
    etapa = models.CharField(max_length=12, choices=ETAPA_CHOICES, db_index=True)
    tabela = models.CharField(max_length=40, blank=True, db_index=True)
    detalhe = models.CharField(max_length=200, blank=True,
                               help_text='Nome do arquivo, do indice ou do contador')
    quantidade = models.BigIntegerField(
        default=0,
        help_text='Linhas carregadas/removidas (0 quando nao se aplica)')
    duracao_s = models.FloatField(default=0.0)
    situacao = models.CharField(max_length=15, choices=SITUACAO_CHOICES,
                                default=SITUACAO_CONCLUIDA, db_index=True,
                                help_text='Em andamento enquanto a etapa roda')
    registrado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'etapa da carga'
        verbose_name_plural = 'etapas da carga'
        ordering = ['carga', 'id']
        constraints = [
            models.UniqueConstraint(
                fields=['carga', 'etapa', 'tabela', 'detalhe'],
                name='uniq_etapa_por_carga'),
        ]
        indexes = [models.Index(fields=['etapa', 'tabela'])]

    def __str__(self):
        alvo = self.detalhe or self.tabela or '-'
        return f'{self.get_etapa_display()} | {alvo} ({self.duracao_s:.1f}s)'

    @property
    def em_andamento(self):
        return self.situacao == self.SITUACAO_ANDAMENTO

    @property
    def segundos_decorridos(self):
        """Duracao da etapa: fechada usa duracao_s; aberta, o relogio."""
        if not self.em_andamento:
            return self.duracao_s
        return max((timezone.now() - self.registrado_em).total_seconds(), 0.0)

    @property
    def linhas_por_segundo(self):
        if not self.duracao_s or not self.quantidade:
            return None
        return self.quantidade / self.duracao_s

