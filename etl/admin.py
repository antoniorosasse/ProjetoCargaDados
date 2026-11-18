"""Admin das estatisticas de carga.

Alem do CRUD das Cargas (com as etapas em inline), existe uma pagina propria:
    /admin/etl/carga/estatisticas/?periodo=dia|semana|mes
que agrega os tempos e as linhas por periodo, com as colunas acumuladas.
"""

from django.contrib import admin, messages
from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path
from django.utils import timezone

from etl import execucao, publicacao
from etl.models import Carga, EtapaCarga


PERIODOS = [('dia', 'Dia'), ('semana', 'Semana'), ('mes', 'Mes')]
ETAPAS_CARGA_DADOS = (EtapaCarga.CARREGAR,)
ETAPAS_INDICES = (EtapaCarga.INDICE, EtapaCarga.ANALYZE)

# Entrada fixa no indice do admin: a pagina de estatisticas nao e um model,
# entao sem isso ela so seria alcancavel pela URL ou pelo botao em Cargas.
admin.site.index_template = 'admin/etl_carga_index.html'


# ------------------------------------------------------------------ formatacao

def fmt_duracao(segundos):
    segundos = float(segundos or 0)
    if segundos < 60:
        return f'{segundos:.1f}s'
    minutos, resto = divmod(int(round(segundos)), 60)
    horas, minutos = divmod(minutos, 60)
    if horas:
        return f'{horas}h{minutos:02d}'
    return f'{minutos}min {resto:02d}s'


def fmt_numero(valor):
    return f'{int(valor or 0):,}'.replace(',', '.')


def fmt_velocidade(valor):
    if not valor:
        return '-'
    return f'{int(valor):,} lin/s'.replace(',', '.')


# ------------------------------------------------------------------ agregacao

def _chave_periodo(dia, granularidade):
    """(chave_ordenavel, rotulo) do periodo de uma data"""
    if granularidade == 'dia':
        return (dia.year, dia.month, dia.day), f'{dia:%d/%m/%Y}'
    if granularidade == 'semana':
        ano, semana, _ = dia.isocalendar()
        return (ano, semana), f'Semana {semana:02d}/{ano}'
    return (dia.year, dia.month), f'{dia:%m/%Y}'


def _grupo_vazio(chave, rotulo):
    return {'chave': chave, 'rotulo': rotulo, 'cargas': 0, 'linhas': 0,
            'tempo_total': 0.0, 'tempo_carga': 0.0, 'tempo_indices': 0.0,
            'tempo_outros': 0.0}


def agregar_por_periodo(cargas, granularidade='mes'):
    """Agrega as cargas por periodo e acumula (linhas, tempo e nº de cargas)"""
    grupos = {}
    for carga in cargas:
        local = timezone.localtime(carga.iniciado_em)
        chave, rotulo = _chave_periodo(local.date(), granularidade)
        grupo = grupos.setdefault(chave, _grupo_vazio(chave, rotulo))
        grupo['cargas'] += 1
        grupo['linhas'] += carga.linhas_carregadas
        grupo['tempo_total'] += carga.duracao_s
        grupo['tempo_carga'] += carga.tempo_carga_s
        grupo['tempo_indices'] += carga.tempo_indices_s

    linhas = sorted(grupos.values(), key=lambda g: g['chave'])
    acumulado_linhas = acumulado_tempo = 0.0
    acumulado_cargas = 0
    for grupo in linhas:
        grupo['tempo_outros'] = max(
            grupo['tempo_total'] - grupo['tempo_carga'] - grupo['tempo_indices'], 0.0)
        grupo['velocidade'] = (grupo['linhas'] / grupo['tempo_carga']
                               if grupo['tempo_carga'] else None)

        acumulado_linhas += grupo['linhas']
        acumulado_tempo += grupo['tempo_total']
        acumulado_cargas += grupo['cargas']
        grupo['linhas_acumuladas'] = acumulado_linhas
        grupo['tempo_acumulado'] = acumulado_tempo
        grupo['cargas_acumuladas'] = acumulado_cargas

        for campo, formatador in (
            ('linhas', fmt_numero), ('linhas_acumuladas', fmt_numero),
            ('tempo_total', fmt_duracao), ('tempo_carga', fmt_duracao),
            ('tempo_indices', fmt_duracao), ('tempo_outros', fmt_duracao),
            ('tempo_acumulado', fmt_duracao), ('velocidade', fmt_velocidade),
        ):
            grupo[f'{campo}_fmt'] = formatador(grupo[campo])
    return linhas


def _linhas_de_carga(cargas):
    """Versao 'pronta para o template' de cada carga (com strings formatadas)"""
    saida = []
    for carga in cargas:
        saida.append({
            'obj': carga,
            'rotulo': carga.rotulo,
            'iniciado_em': timezone.localtime(carga.iniciado_em),
            'status': carga.get_status_display(),
            'duracao': fmt_duracao(carga.duracao_s),
            'linhas': fmt_numero(carga.linhas_carregadas),
            'tempo_carga': fmt_duracao(carga.tempo_carga_s),
            'tempo_indices': fmt_duracao(carga.tempo_indices_s),
            'tempo_contadores': fmt_duracao(carga.tempo_contadores_s),
            'velocidade': fmt_velocidade(carga.linhas_por_segundo),
            'n_etapas': len(carga._etapas()),
        })
    return saida


def _totais(cargas):
    """Totais acumulados de todas as cargas"""
    linhas = sum(c.linhas_carregadas for c in cargas)
    tempo_total = sum(c.duracao_s for c in cargas)
    tempo_carga = sum(c.tempo_carga_s for c in cargas)
    tempo_indices = sum(c.tempo_indices_s for c in cargas)
    tempo_contadores = sum(c.tempo_contadores_s for c in cargas)
    return {
        'cargas': len(cargas),
        'linhas': fmt_numero(linhas),
        'tempo_total': fmt_duracao(tempo_total),
        'tempo_carga': fmt_duracao(tempo_carga),
        'tempo_indices': fmt_duracao(tempo_indices),
        'tempo_contadores': fmt_duracao(tempo_contadores),
        'velocidade': fmt_velocidade(linhas / tempo_carga if tempo_carga else None),
        'indices': sum(1 for c in cargas for e in c._etapas()
                       if e.etapa == EtapaCarga.INDICE),
    }


def _ranking_indices(cargas, limite=15):
    """Indices mais caros do historico (todas as cargas)"""
    acumulado = {}
    for carga in cargas:
        for etapa in carga._etapas():
            if etapa.etapa != EtapaCarga.INDICE:
                continue
            chave = (etapa.tabela, etapa.detalhe)
            item = acumulado.setdefault(chave, {'tabela': etapa.tabela,
                                                'detalhe': etapa.detalhe,
                                                'duracao_s': 0.0, 'vezes': 0})
            item['duracao_s'] += etapa.duracao_s
            item['vezes'] += 1
    ranking = sorted(acumulado.values(), key=lambda i: i['duracao_s'], reverse=True)
    for item in ranking[:limite]:
        item['duracao_fmt'] = fmt_duracao(item['duracao_s'])
    return ranking[:limite]


def _resumo_tabelas(carga):
    """Etapas da ultima carga agrupadas por tabela (carga, indices, contadores)"""
    if carga is None:
        return []
    tabelas = {}
    for etapa in carga._etapas():
        if not etapa.tabela:
            continue
        item = tabelas.setdefault(etapa.tabela, {
            'tabela': etapa.tabela, 'linhas': 0, 'tempo_carga': 0.0,
            'tempo_indices': 0.0, 'n_indices': 0, 'tempo_contador': 0.0,
        })
        if etapa.etapa in (EtapaCarga.CARREGAR, EtapaCarga.RESTAURAR):
            item['linhas'] += etapa.quantidade
            item['tempo_carga'] += etapa.duracao_s
        elif etapa.etapa == EtapaCarga.INDICE:
            item['tempo_indices'] += etapa.duracao_s
            item['n_indices'] += 1
        elif etapa.etapa == EtapaCarga.CONTADOR:
            item['tempo_contador'] += etapa.duracao_s

    saida = sorted(tabelas.values(), key=lambda i: i['linhas'], reverse=True)
    for item in saida:
        item['linhas_fmt'] = fmt_numero(item['linhas']) if item['linhas'] else '-'
        item['tempo_carga_fmt'] = (fmt_duracao(item['tempo_carga'])
                                   if item['tempo_carga'] else '-')
        item['tempo_indices_fmt'] = (fmt_duracao(item['tempo_indices'])
                                     if item['n_indices'] else '-')
        item['tempo_contador_fmt'] = (fmt_duracao(item['tempo_contador'])
                                      if item['tempo_contador'] else '-')
        item['velocidade'] = fmt_velocidade(
            item['linhas'] / item['tempo_carga'] if item['tempo_carga'] else None)
    return saida


def _andamento(carga):
    """Etapas abertas da carga corrente -- o que esta rodando agora."""
    if carga is None:
        return []
    arquivos = {}
    for etapa in carga._etapas():
        if etapa.etapa == EtapaCarga.ARQUIVO:
            arquivos[etapa.tabela] = arquivos.get(etapa.tabela, 0) + 1

    saida = []
    for etapa in carga.etapas_abertas:
        saida.append({
            'etapa': etapa.get_etapa_display(),
            'tabela': etapa.tabela or '-',
            'detalhe': etapa.detalhe,
            'tempo': fmt_duracao(etapa.segundos_decorridos),
            'arquivos': arquivos.get(etapa.tabela, 0),
        })
    return saida


# ----------------------------------------------------------------- admin

class EtapaCargaInline(admin.TabularInline):
    model = EtapaCarga
    extra = 0
    can_delete = False
    ordering = ('id',)
    fields = ('etapa', 'tabela', 'detalhe', 'quantidade', 'duracao_fmt',
              'linhas_por_segundo_fmt', 'situacao')
    readonly_fields = fields

    def has_add_permission(self, request, obj=None):
        return False

    @admin.display(description='duracao')
    def duracao_fmt(self, obj):
        return fmt_duracao(obj.duracao_s)

    @admin.display(description='lin/s')
    def linhas_por_segundo_fmt(self, obj):
        return fmt_velocidade(obj.linhas_por_segundo)


@admin.register(Carga)
class CargaAdmin(admin.ModelAdmin):
    list_display = ('rotulo', 'iniciado_em', 'status', 'tabela_fmt',
                    'registros_fmt', 'andamento_fmt', 'duracao_fmt',
                    'linhas_fmt', 'tempo_carga_fmt', 'tempo_indices_fmt',
                    'velocidade_fmt')
    list_filter = ('status', 'rotulo')
    search_fields = ('rotulo', 'pasta', 'host', 'observacao')
    date_hierarchy = 'iniciado_em'
    inlines = [EtapaCargaInline]
    change_list_template = 'admin/etl/carga/change_list.html'
    readonly_fields = ('registrado_em', 'tabela_fmt', 'registros_fmt',
                       'andamento_fmt', 'duracao_fmt',
                       'linhas_fmt', 'tempo_carga_fmt', 'tempo_indices_fmt',
                       'velocidade_fmt')

    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related('etapas')

    # ------------------------------------------------------ colunas calculadas

    @admin.display(description='tabela agora')
    def tabela_fmt(self, obj):
        """Tabela da etapa aberta (o que esta sendo processado neste momento)"""
        abertas = obj.etapas_abertas
        if obj.status != Carga.STATUS_EM_ANDAMENTO or not abertas:
            return '-'
        return abertas[0].tabela or '-'

    @admin.display(description='registros agora')
    def registros_fmt(self, obj):
        """Linhas ja migradas: etapas concluidas + o parcial da etapa aberta"""
        total = obj.linhas_carregadas
        if obj.status == Carga.STATUS_EM_ANDAMENTO:
            abertas = obj.etapas_abertas
            if abertas and abertas[0].quantidade:
                total += abertas[0].quantidade
        return fmt_numero(total) if total else '-'

    @admin.display(description='andamento')
    def andamento_fmt(self, obj):
        if obj.status != Carga.STATUS_EM_ANDAMENTO:
            return '-'
        abertas = obj.etapas_abertas
        if not abertas:
            return '⏳ iniciando'
        etapa = abertas[0]
        partes = [p for p in (etapa.tabela, etapa.detalhe) if p]
        alvo = ' · '.join(partes) or etapa.get_etapa_display()
        return f'⏳ {alvo} ({fmt_duracao(etapa.segundos_decorridos)})'

    @admin.display(description='duracao')
    def duracao_fmt(self, obj):
        return fmt_duracao(obj.duracao_s)

    @admin.display(description='linhas')
    def linhas_fmt(self, obj):
        return fmt_numero(obj.linhas_carregadas)

    @admin.display(description='tempo de carga')
    def tempo_carga_fmt(self, obj):
        return fmt_duracao(obj.tempo_carga_s)

    @admin.display(description='tempo de indices')
    def tempo_indices_fmt(self, obj):
        return fmt_duracao(obj.tempo_indices_s)

    @admin.display(description='lin/s')
    def velocidade_fmt(self, obj):
        return fmt_velocidade(obj.linhas_por_segundo)

    # ------------------------------------------------------------ pagina extra

    def get_urls(self):
        urls = super().get_urls()
        extras = [
            path('estatisticas/',
                 self.admin_site.admin_view(self.estatisticas_view),
                 name='etl_carga_estatisticas'),
            path('executar/',
                 self.admin_site.admin_view(self.executar_view),
                 name='etl_carga_executar'),
        ]
        return extras + urls

    def has_add_permission(self, request):
        # "Adicionar carga" criaria uma carga fantasma (sem etapas, sem fim)
        # e a pagina passaria a mostrar "carga em andamento" para sempre.
        # Carga nasce do comando; a execucao daqui e o botao "Executar carga".
        return False

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['url_estatisticas'] = 'estatisticas/'
        extra_context['url_executar'] = 'executar/'
        return super().changelist_view(request, extra_context=extra_context)

    def executar_view(self, request):
        """Formulario que dispara `carregar_base_cnpj` em segundo plano"""
        if not request.user.is_superuser:
            raise PermissionDenied('Somente superusuario executa a carga.')

        # Import tardio: o comando importa o processador (pandas/pyarrow) e o
        # admin nao deve pagar esse custo de memoria em toda requisicao.
        from etl.management.commands.carregar_base_cnpj import Command as ComandoCarga
        from etl.management.commands.publicar_base_cnpj import Command as ComandoPublicacao

        if request.method == 'POST':
            acao = request.POST.get('acao', 'disparar')
            try:
                if acao == 'parar':
                    encerrou = execucao.parar()
                    messages.success(
                        request,
                        'Carga encerrada.' if encerrou
                        else 'Nao havia carga em execucao.')
                elif acao == 'parar_publicacao':
                    encerrou = publicacao.parar()
                    messages.success(
                        request,
                        'Publicacao encerrada.' if encerrou
                        else 'Nao havia publicacao em execucao.')
                elif acao == 'checar_prod':
                    linhas = publicacao.checar()
                    request.session['publicacao_checagem'] = linhas
                    messages.success(request, 'Servidor de producao respondeu.')
                elif acao == 'publicar':
                    fase_pub = request.POST.get('fase_pub', 'tudo')
                    tabelas_pub = request.POST.getlist('tabelas_prod')
                    if not tabelas_pub and fase_pub != 'status':
                        raise ValueError('Marque pelo menos uma tabela.')
                    confirmacao = request.POST.get(
                        'confirmacao_prod', '').strip().upper()
                    if (fase_pub in publicacao.FASES_COM_CONFIRMACAO
                            and confirmacao != 'PUBLICAR'):
                        raise ValueError(
                            'Restaurar/indices/tudo apagam dados na VPS: '
                            'digite PUBLICAR no campo de confirmacao.')
                    dados = publicacao.disparar(
                        rotulo=request.POST.get('rotulo_pub', ''),
                        fase=fase_pub,
                        tabelas=tabelas_pub,
                        jobs=request.POST.get('jobs_pub') or 4,
                        parar_servico=request.POST.get('parar_servico') == 'on',
                        backup=request.POST.get('backup_prod') == 'on',
                    )
                    messages.success(
                        request,
                        f'Publicacao disparada (pid {dados["pid"]}). '
                        f'Log: {dados["log"]}')
                else:
                    fase = request.POST.get('fase', 'tudo')
                    tabelas = request.POST.getlist('tabelas')
                    if not tabelas:
                        raise ValueError('Marque pelo menos uma tabela.')
                    confirmacao = request.POST.get('confirmacao', '').strip().upper()
                    if fase in ('ddl', 'truncar', 'tudo') and confirmacao != 'CARREGAR':
                        raise ValueError(
                            'Para DDL/TRUNCATE/recarga completa digite CARREGAR '
                            'no campo de confirmacao.')
                    dados = execucao.disparar(
                        pasta=request.POST.get('pasta', ''),
                        fase=fase,
                        tabelas=tabelas,
                        rotulo=request.POST.get('rotulo', ''),
                        batch=request.POST.get('batch') or 50000,
                        workers=request.POST.get('workers') or 4,
                        limit=request.POST.get('limit') or 0,
                    )
                    messages.success(
                        request,
                        f'Carga disparada (pid {dados["pid"]}). '
                        f'Log: {dados["log"]}')
            except (ValueError, TypeError, OSError) as exc:
                messages.error(request, str(exc))
            return redirect('admin:etl_carga_executar')

        atual = execucao.estado()
        pasta_sel = request.GET.get('pasta') or (atual or {}).get('pasta', '')
        arquivos = {}
        if pasta_sel:
            try:
                arquivos = execucao.contar_arquivos(
                    execucao.validar_pasta(pasta_sel))
            except ValueError:
                arquivos = {}

        contexto = dict(
            self.admin_site.each_context(request),
            title='Executar carga',
            titulo='Executar carga da base CNPJ',
            opts=self.model._meta,
            fases=execucao.FASES,
            tabelas=ComandoCarga.TABELAS_ORDEM,
            tabelas_dominio=ComandoCarga.TABELAS_DOMINIO,
            tabelas_grandes=ComandoCarga.TABELAS_GRANDES,
            pastas=execucao.listar_pastas(),
            raizes=[str(raiz) for raiz in execucao.raizes()],
            estado=atual,
            pasta_sel=pasta_sel,
            arquivos=arquivos,
            log_dir=str(execucao.diretorio()),
            ddl_padrao=ComandoCarga.DDL_PADRAO,
            # ---- publicacao na VPS
            publicacao=publicacao.estado(),
            publicacao_fases=publicacao.FASES,
            publicacao_ssh=settings.PUBLICACAO_SSH,
            publicacao_remoto=settings.PUBLICACAO_REMOTO,
            publicacao_servico=settings.PUBLICACAO_SERVICO,
            publicacao_dumps=str(settings.PUBLICACAO_DUMP_DIR),
            publicacao_jobs=settings.PUBLICACAO_JOBS,
            checagem=request.session.get('publicacao_checagem'),
            tabelas_prod=ComandoPublicacao.TABELAS_ORDEM,
            tabelas_prod_dominio=ComandoPublicacao.TABELAS_DOMINIO,
            tabelas_prod_grandes=ComandoPublicacao.TABELAS_GRANDES,
            tabelas_prod_derivadas=ComandoPublicacao.TABELAS_DERIVADAS,
        )
        return TemplateResponse(request, 'admin/etl/carga/executar.html', contexto)

    def estatisticas_view(self, request):
        periodo = request.GET.get('periodo', 'mes')
        if periodo not in dict(PERIODOS):
            periodo = 'mes'

        cargas = list(Carga.objects.prefetch_related('etapas').order_by('iniciado_em'))
        grupos = agregar_por_periodo(cargas, periodo)
        ultima = cargas[-1] if cargas else None
        carga_aberta = (ultima if ultima is not None and
                        ultima.status == Carga.STATUS_EM_ANDAMENTO else None)

        contexto = dict(
            self.admin_site.each_context(request),
            title='Estatisticas das cargas',
            titulo='Estatisticas das cargas',
            periodos=PERIODOS,
            periodo=periodo,
            grupos=grupos,
            cargas=_linhas_de_carga(list(reversed(cargas))),
            totais=_totais(cargas),
            ranking_indices=_ranking_indices(cargas),
            resumo_tabelas=_resumo_tabelas(ultima),
            ultima=ultima,
            carga_aberta=carga_aberta,
            andamento=_andamento(carga_aberta),
            andamento_total=(fmt_duracao(carga_aberta.duracao_s)
                             if carga_aberta else ''),
            opts=self.model._meta,
        )
        return TemplateResponse(request, 'admin/etl/carga/estatisticas.html', contexto)



