"""Registro das estatisticas de carga no Postgres django_internal (best-effort).

O banco e o mesmo do admin do cnpj_interface (settings.DATABASES['default']).
Nenhuma funcao daqui pode derrubar uma carga: qualquer erro vira aviso no
stdout e o fluxo segue. A carga dura horas; a estatistica e acessoria.
"""

import atexit
import os
import signal
import socket

from django.utils import timezone

from etl.models import Carga, EtapaCarga

# Cargas abertas neste processo: se ele morrer no meio (Ctrl+C, SIGTERM do
# "parar" do admin, excecao nao tratada), o atexit/handler fecha a carga em
# vez de deixa-la "em andamento" para sempre na pagina de estatisticas.
_cargas_abertas = set()


def _encerrar_por_sinal(signum, frame):
    """SIGTERM/SIGHUP: fecha a carga e sai pelo caminho normal (dispara atexit)"""
    fechar_cargas_pendentes()
    raise SystemExit(128 + signum)


def _instalar_handlers():
    if getattr(_instalar_handlers, 'feito', False):
        return
    _instalar_handlers.feito = True
    for sinal in (signal.SIGTERM, signal.SIGHUP):
        try:
            signal.signal(sinal, _encerrar_por_sinal)
        except (ValueError, OSError):
            # fora da thread principal ou plataforma sem o sinal: segue sem ele
            pass


def iniciar_carga(rotulo, pasta='', observacao='', host=None):
    """Cria a Carga da execucao. Devolve None (e avisa) se o banco falhar."""
    try:
        carga = Carga.objects.create(
            rotulo=(rotulo or 'sem-rotulo')[:60],
            pasta=(pasta or '')[:255],
            host=(host or socket.gethostname())[:80],
            iniciado_em=timezone.now(),
            observacao=observacao,
        )
        _cargas_abertas.add(carga.pk)
        _instalar_handlers()
        return carga
    except Exception as exc:  # pragma: no cover - caminho de falha do banco
        print(f'   ⚠️  estatisticas desativadas nesta execucao: {exc}')
        return None


@atexit.register
def fechar_cargas_pendentes():
    """Fecha carga/etapa abertas quando o processo termina sem concluir."""
    if not _cargas_abertas:
        return
    try:
        EtapaCarga.objects.filter(
            carga_id__in=list(_cargas_abertas),
            situacao=EtapaCarga.SITUACAO_ANDAMENTO,
        ).update(situacao=EtapaCarga.SITUACAO_CONCLUIDA)
        Carga.objects.filter(
            pk__in=list(_cargas_abertas),
            status=Carga.STATUS_EM_ANDAMENTO,
        ).update(status=Carga.STATUS_FALHOU, finalizado_em=timezone.now())
    except Exception:  # pragma: no cover - banco fora do ar no encerramento
        pass
    finally:
        _cargas_abertas.clear()


def _chave(carga, etapa, tabela, detalhe):
    return dict(
        carga=carga,
        etapa=etapa,
        tabela=(tabela or '')[:40],
        detalhe=(detalhe or '')[:200],
    )


def iniciar_etapa(carga, etapa, tabela='', detalhe=''):
    """Abre a etapa como 'em andamento' antes de executa-la.

    A pagina de estatisticas mostra a etapa aberta com o relogio correndo,
    o que da o andamento real de uma fase longa (um indice leva ~40 min).
    """
    if carga is None:
        return None
    try:
        objeto, _ = EtapaCarga.objects.update_or_create(
            **_chave(carga, etapa, tabela, detalhe),
            defaults={'situacao': EtapaCarga.SITUACAO_ANDAMENTO},
        )
        return objeto
    except Exception as exc:  # pragma: no cover - caminho de falha do banco
        print(f'   ⚠️  inicio da etapa {etapa} {tabela} {detalhe} '
              f'nao registrado: {exc}')
        return None


def registrar(carga, etapa, tabela='', detalhe='', quantidade=0, duracao_s=0.0):
    """Grava/fecha uma etapa. Idempotente por (carga, etapa, tabela, detalhe)."""
    if carga is None:
        return None
    try:
        objeto, _ = EtapaCarga.objects.update_or_create(
            **_chave(carga, etapa, tabela, detalhe),
            defaults={
                'quantidade': int(quantidade or 0),
                'duracao_s': round(float(duracao_s or 0.0), 3),
                'situacao': EtapaCarga.SITUACAO_CONCLUIDA,
            },
        )
        return objeto
    except Exception as exc:  # pragma: no cover - caminho de falha do banco
        print(f'   ⚠️  etapa {etapa} {tabela} {detalhe} nao registrada: {exc}')
        return None


def concluir_carga(carga, status=Carga.STATUS_CONCLUIDA):
    if carga is None:
        return
    _cargas_abertas.discard(carga.pk)
    try:
        carga.status = status
        carga.finalizado_em = timezone.now()
        carga.save(update_fields=['status', 'finalizado_em'])
        # Uma carga que terminou nao pode deixar etapa aberta na pagina.
        EtapaCarga.objects.filter(
            carga=carga, situacao=EtapaCarga.SITUACAO_ANDAMENTO,
        ).update(situacao=EtapaCarga.SITUACAO_CONCLUIDA)
    except Exception as exc:  # pragma: no cover - caminho de falha do banco
        print(f'   ⚠️  nao foi possivel fechar a carga: {exc}')

