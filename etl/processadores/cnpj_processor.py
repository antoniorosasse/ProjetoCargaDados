import pandas as pd
import logging
import gc
import os
import resource
import io
import re
import csv
import time
import zipfile
from datetime import datetime, timedelta
from django.conf import settings
from etl.conexoes.postgres import get_postgres_connection
from etl.conexoes.mongo import get_mongo_database
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Mapeamento nome do arquivo da RFB -> tabela. Usado pelo importar_cnpj (para
# agrupar os arquivos) e pelo carregar_base_cnpj (para paralelizar por arquivo).
PADROES_ARQUIVO_TABELA = {
    r'^C_Cnaes': 'c_cnaes', r'^C_Empresas': 'c_empresas',
    r'^C_Estabelecimentos': 'c_estabelecimentos', r'^C_Motivos': 'c_motivos',
    r'^C_Municipios': 'c_municipios', r'^C_Naturezas': 'c_naturezas',
    r'^C_Paises': 'c_paises', r'^C_Qualificacoes': 'c_qualificacoes',
    r'^C_Simples': 'c_simples', r'^C_Socios': 'c_socios',
    r'^Cnaes': 'c_cnaes', r'^Empresas': 'c_empresas',
    r'^Estabelecimentos': 'c_estabelecimentos', r'^Motivos': 'c_motivos',
    r'^Municipios': 'c_municipios', r'^Naturezas': 'c_naturezas',
    r'^Paises': 'c_paises', r'^Qualificacoes': 'c_qualificacoes',
    r'^Simples': 'c_simples', r'^Socios': 'c_socios',
}

# Aumentar limite de memória (se possível)
try:
    resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
except:
    pass

class CNPJProcessor:
    """Processa arquivos Parquet de CNPJ com leitura em chunks e COPY (otimizado)"""
    
    TABELAS_CONFIG = {
        'c_cnaes': {
            'colunas': ['cnae', 'nm_cnae'],
            'chave': 'cnae',
            'nome_amigavel': 'CNAEs',
            'campo_id': 'cnae'
        },
        'c_empresas': {
            'colunas': ['cnpj_basico', 'razao_social', 'nat_juridica', 'qual_resp', 
                       'capital_social', 'porte_empresa', 'ente_fed_resp'],
            'chave': 'cnpj_basico',
            'nome_amigavel': 'Empresas',
            'campo_id': 'id_empresa'
        },
        'c_estabelecimentos': {
            'colunas': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'cd_matriz_filial', 
                       'nm_fantasia', 'cd_sit_cadastro', 'dt_sit_cadastro', 'motivo_sit_cadastro',
                       'nm_cidade_ext', 'cd_pais', 'dt_ini', 'cnae_principal', 'cnae_secundario',
                       'tip_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep',
                       'uf', 'municipio', 'ddd_1', 'tel_1', 'ddd_2', 'tel_2', 'ddd_fax', 'fax',
                       'email', 'sit_esp', 'dt_sit_esp'],
            'chave': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'],
            'nome_amigavel': 'Estabelecimentos',
            'campo_id': None,
            'usar_chave_composta': True
        },
        'c_motivos': {
            'colunas': ['cd_motivo_sit_cadastro', 'motivo_sit_cadastro'],
            'chave': 'cd_motivo_sit_cadastro',
            'nome_amigavel': 'Motivos',
            'campo_id': 'cd_motivo_sit_cadastro'
        },
        'c_municipios': {
            'colunas': ['cd_municipio', 'municipio'],
            'chave': 'cd_municipio',
            'nome_amigavel': 'Municipios',
            'campo_id': 'cd_municipio'
        },
        'c_naturezas': {
            'colunas': ['cd_nat_juridica', 'nat_juridica'],
            'chave': 'cd_nat_juridica',
            'nome_amigavel': 'Naturezas',
            'campo_id': 'cd_nat_juridica'
        },
        'c_paises': {
            'colunas': ['cd_pais', 'pais'],
            'chave': 'cd_pais',
            'nome_amigavel': 'Paises',
            'campo_id': 'cd_pais'
        },
        'c_qualificacoes': {
            'colunas': ['cd_quals', 'nm_quals'],
            'chave': 'cd_quals',
            'nome_amigavel': 'Qualificacoes',
            'campo_id': 'cd_quals'
        },
        'c_simples': {
            'colunas': ['cnpj_basico', 'op_simples', 'dt_op_simples', 'dt_exc_simples',
                       'op_mei', 'dt_op_mei', 'dt_exc_mei'],
            'chave': ['cnpj_basico', 'dt_op_simples'],
            'nome_amigavel': 'Simples',
            'campo_id': 'id_simples'
        },
        'c_socios': {
            'colunas': ['cnpj_basico', 'id_tipo_socio', 'nome_ou_razao_social', 'cnpj_cpf',
                       'qualif_socio', 'dt_entrada', 'cd_pais', 'repr_legal', 'nm_repr',
                       'cd_qualif_repr', 'faixa_etaria'],
            'chave': ['cnpj_basico', 'id_tipo_socio', 'cnpj_cpf'],
            'nome_amigavel': 'Socios',
            'campo_id': 'id_socio'
        }
    }
    
    CONFLICT_STRATEGIES = ['update', 'nothing', 'none']

    # Quantidade de campos de cada tabela na base publica da RFB (CSV ';' com aspas).
    # Serve de guarda de layout: se o arquivo vier com numero diferente de campos,
    # a carga para na hora em vez de gravar coluna trocada.
    CAMPOS_ESPERADOS = {
        'c_cnaes': 2,
        'c_empresas': 7,
        'c_estabelecimentos': 30,
        'c_motivos': 2,
        'c_municipios': 2,
        'c_naturezas': 2,
        'c_paises': 2,
        'c_qualificacoes': 2,
        'c_simples': 7,
        'c_socios': 11,
    }
    
    def __init__(self, arquivo_path, tabela_nome, batch_size=10000, progress_interval=50000, 
                 conflict_strategy='none', test_limit=0, dump=False, filtro=None, formato='auto'):
        self.arquivo_path = arquivo_path
        self.tabela_nome = tabela_nome
        self.formato = formato
        self.limpar_texto = False  # ligado apenas no destino mongo (ver processar_em_chunks)
        self.batch_size = batch_size
        self.progress_interval = progress_interval
        self.conflict_strategy = conflict_strategy
        self.test_limit = test_limit
        self.dump = dump
        self.filtro = filtro
        self.nome_arquivo = arquivo_path.split('/')[-1]
        
        self.config = self.TABELAS_CONFIG.get(tabela_nome)
        if not self.config:
            raise ValueError(f"Tabela {tabela_nome} não configurada!")
        
        self.colunas = self.config.get('colunas', [])
        self.chave = self.config.get('chave')
        self.nome_amigavel = self.config.get('nome_amigavel', tabela_nome)
        self.campo_id = self.config.get('campo_id')
        self.usar_chave_composta = self.config.get('usar_chave_composta', False)
        
        self.registros_pulados = []
        self.ultimo_dump = 0
    
    def _write(self, msg):
        print(msg)
    
    def _limpar_texto(self, valor):
        """Limpa texto: remove caracteres especiais, normaliza encoding (SEM TRUNCAR)"""
        if pd.isna(valor):
            return None
        if isinstance(valor, (int, float)):
            return str(valor)
        texto = str(valor)
        # Remove caracteres de controle
        texto = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]', '', texto)
        # Remove barras invertidas (causam problemas no COPY do PostgreSQL)
        texto = texto.replace('\\', '')
        # Remove tabs
        texto = texto.replace('\t', ' ')
        # Remove quebras de linha
        texto = texto.replace('\n', ' ').replace('\r', '')
        # Normaliza encoding
        texto = texto.encode('utf-8', errors='ignore').decode('utf-8')
        # Remove espaços extras
        texto = ' '.join(texto.split())
        return texto if texto else None

    def _limpar_serie_texto(self, serie):
        """Limpeza de texto vetorial (mesmo resultado do _limpar_texto, sem apply).

        O apply celula-a-celula era o gargalo da carga (4 mil lin/s). Aqui a
        limpeza roda em duas passadas do motor de strings do pandas.
        """
        if serie.dtype != 'object':
            return serie
        s = serie.astype(str)
        vazio = serie.isna() | (s == '')
        # caracteres de controle + barra invertida (quebram o COPY do PostgreSQL)
        s = s.str.replace(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f\\]', '', regex=True)
        # tabs/quebras de linha viram espaco e espacos multiplos sao colapsados
        s = s.str.replace(r'\s+', ' ', regex=True).str.strip()
        s = s.where(~(vazio | (s == '')), None)
        return s

    def _detectar_formato(self):
        """Resolve o formato do arquivo: parquet, csv dentro de zip, ou csv solto"""
        if self.formato and self.formato != 'auto':
            return self.formato
        ext = os.path.splitext(self.arquivo_path)[1].lower()
        if ext == '.parquet':
            return 'parquet'
        if ext == '.zip':
            return 'csv'
        return 'csv'

    def _abrir_handle_csv(self):
        """Abre o CSV (zip aninhado ou arquivo solto) e devolve um TextIOWrapper latin-1"""
        caminho = self.arquivo_path
        if caminho.lower().endswith('.zip'):
            self._zip_arquivo = open(caminho, 'rb')
            zf = zipfile.ZipFile(self._zip_arquivo)
            self._zip = zf
            membros = [n for n in zf.namelist() if not n.endswith('/')]
            if not membros:
                raise ValueError(f'Zip sem arquivos de dados: {caminho}')
            self._zip_membro = membros[0]
            bruto = zf.open(self._zip_membro)
        else:
            bruto = open(caminho, 'rb')
        return io.TextIOWrapper(bruto, encoding='latin-1', newline='')

    def _validar_layout(self):
        """Le a primeira linha e confere a quantidade de campos esperada da tabela"""
        esperado = self.CAMPOS_ESPERADOS.get(self.tabela_nome)
        if not esperado:
            return
        handle = self._abrir_handle_csv()
        try:
            primeira = handle.readline()
        finally:
            handle.close()
            self._fechar_zip()
        campos = next(csv.reader([primeira], delimiter=';', quotechar='"'), [])
        if len(campos) != esperado:
            raise ValueError(
                f'Layout inesperado em {self.nome_arquivo}: {len(campos)} campos, '
                f'esperado {esperado} para {self.tabela_nome}'
            )

    def _fechar_zip(self):
        """Fecha zip/membro abertos em _abrir_handle_csv"""
        zf = getattr(self, '_zip', None)
        if zf is not None:
            try:
                zf.close()
            except Exception:
                pass
            self._zip = None
        bruto = getattr(self, '_zip_arquivo', None)
        if bruto is not None:
            try:
                bruto.close()
            except Exception:
                pass
            self._zip_arquivo = None

    def _iterar_chunks(self):
        """Gerador de chunks na ordem do arquivo + total de registros (None se desconhecido)"""
        formato = self._detectar_formato()
        colunas = [col.upper() for col in self.colunas] if self.colunas else None

        if formato == 'parquet':
            parquet_file = pq.ParquetFile(self.arquivo_path)
            total = parquet_file.metadata.num_rows

            def gerar_parquet():
                for batch in parquet_file.iter_batches(batch_size=self.batch_size, columns=colunas):
                    yield batch.to_pandas()

            return gerar_parquet(), total

        # CSV: guarda de layout na primeira linha, depois leitura em chunks
        self._validar_layout()
        handle = self._abrir_handle_csv()
        leitor = pd.read_csv(
            handle,
            sep=';',
            quotechar='"',
            header=None,
            names=colunas,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            chunksize=self.batch_size,
            engine='c',
        )

        def gerar_csv():
            try:
                for chunk in leitor:
                    yield chunk
            finally:
                handle.close()
                self._fechar_zip()

        return gerar_csv(), None

    
    def _limpar_uf(self, valor):
        if pd.isna(valor):
            return None
        texto = str(valor).strip()
        texto = re.sub(r'[^a-zA-Z]', '', texto)
        if texto:
            return texto[:2].upper()
        return None
    
    def _salvar_dump(self, registros, suffix=""):
        if not registros:
            return None
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f'pulados_{self.tabela_nome}_{timestamp}{suffix}.csv'
        with open(nome_arquivo, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['_id', 'motivo', 'data_registro']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(registros)
        return nome_arquivo
    
    def _transformar_chunk(self, df, limpar=None):
        """Transforma um chunk de dados - COM CORREÇÃO POR TABELA E FILTRO OTIMIZADO

        limpar: normaliza o texto coluna a coluna (caro). O COPY em FORMAT csv
        ja lida com tab/quebra/aspas, entao a limpeza so e necessaria no mongo.
        """
        
        # 1. NORMALIZAR NOMES DE COLUNAS (MAIÚSCULAS → minúsculas)
        df.columns = [col.lower() for col in df.columns]
        
        # 2. LIMPEZA DE TEXTO PARA TODAS AS COLUNAS (vetorial, uma passada)
        if limpar is None:
            limpar = self.limpar_texto
        if limpar:
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = self._limpar_serie_texto(df[col])
        
        # 2b. CORREÇÃO ESPECÍFICA PARA CNPJ_DV (varchar(2) com zero à esquerda)
        # O Parquet traz cnpj_dv como uint8 (int), então "1" precisa virar "01".
        # O cnpj_dv NÃO pode ir para a lista de colunas numéricas, senão perde o zero.
        # No CSV o DV ja vem como texto ('01'); no Parquet vinha int (1 -> '01').
        # Vazio/ausente vira NULL (o int('') estourava quando o dado vinha do CSV).
        if 'cnpj_dv' in df.columns and self.tabela_nome == 'c_estabelecimentos':
            dv = df['cnpj_dv'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            dv = dv.where(~dv.isin(['', 'nan', 'None', '<NA>']), None)
            df['cnpj_dv'] = dv.str.zfill(2)
        
        # 3. CORREÇÃO ESPECÍFICA PARA UF (vetorial)
        if 'uf' in df.columns:
            uf = df['uf'].astype(str).str.upper().str.replace(r'[^A-Z]', '', regex=True)
            uf = uf.str.slice(0, 2)
            df['uf'] = uf.where(uf != '', None)
        
        # 4. CORREÇÃO PARA COLUNAS QUE DEVEM SER NÚMERO
        colunas_numericas_por_tabela = {
            'c_cnaes': ['cnae'],
            'c_empresas': ['nat_juridica', 'qual_resp'],
            'c_estabelecimentos': [
                'cd_matriz_filial', 'cd_sit_cadastro', 'motivo_sit_cadastro',
                'cd_pais', 'cnae_principal', 'municipio'
            ],
            'c_motivos': ['cd_motivo_sit_cadastro'],
            'c_municipios': ['cd_municipio'],
            'c_naturezas': ['cd_nat_juridica'],
            'c_paises': ['cd_pais'],
            'c_qualificacoes': ['cd_quals'],
            'c_simples': [],
            'c_socios': [
                'id_tipo_socio', 'qualif_socio', 'cd_pais', 
                'cd_qualif_repr', 'faixa_etaria'
            ]
        }
        
        colunas_para_numerico = colunas_numericas_por_tabela.get(self.tabela_nome, [])

        # capital_social no CSV da RFB vem como texto pt-BR ("1.234.567,89") e o
        # COPY nao converte isso para numeric(18,2) -- converte aqui.
        if 'capital_social' in df.columns and df['capital_social'].dtype == 'object':
            cs = df['capital_social'].astype(str).str.strip()
            cs = cs.str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df['capital_social'] = pd.to_numeric(cs, errors='coerce')
        
        for col in colunas_para_numerico:
            if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if df[col].dtype == 'float64':
                    try:
                        df[col] = df[col].astype('Int64')
                    except:
                        pass
        
        # 5. CONVERSÃO DE DATAS (com múltiplos formatos)
        colunas_data = ['dt_sit_cadastro', 'dt_ini', 'dt_sit_esp', 
                       'dt_op_simples', 'dt_exc_simples', 'dt_op_mei', 'dt_exc_mei',
                       'dt_entrada']
        for col in colunas_data:
            if col in df.columns:
                if df[col].dtype == 'datetime64[ns]':
                    continue
                
                # Criar uma série separada
                serie = df[col].copy()
                
                # Se a série já é numérica (AAAAMMDD), converter direto
                if pd.api.types.is_numeric_dtype(serie):
                    df[col] = pd.to_datetime(serie.astype(str), format='%Y%m%d', errors='coerce')
                else:
                    # Se é string, tentar vários formatos
                    serie_str = serie.astype(str)
                    df[col] = pd.to_datetime(serie_str, format='%Y%m%d', errors='coerce')
                    
                    mask = df[col].isna() & (serie_str != 'nan') & (serie_str != 'None') & (serie_str != '')
                    if mask.any():
                        df.loc[mask, col] = pd.to_datetime(serie_str[mask], format='%d/%m/%Y', errors='coerce')
                    
                    mask = df[col].isna() & (serie_str != 'nan') & (serie_str != 'None') & (serie_str != '')
                    if mask.any():
                        df.loc[mask, col] = pd.to_datetime(serie_str[mask], errors='coerce')
        
        # 6. TRATAR NULOS (SEM usar df.where!)
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].where(pd.notnull(df[col]), None)
        
        # 🔥 7. APLICAR FILTRO (OTIMIZADO - sem df.query())
        if self.filtro:
            try:
                if '!=' in self.filtro:
                    col, valor = self.filtro.split('!=')
                    col = col.strip()
                    valor = valor.strip()
                    
                    if col in df.columns:
                        try:
                            valor_num = int(valor)
                            df = df[df[col] != valor_num]
                        except:
                            df = df[df[col].astype(str) != valor]
                elif '==' in self.filtro:
                    col, valor = self.filtro.split('==')
                    col = col.strip()
                    valor = valor.strip()
                    
                    if col in df.columns:
                        try:
                            valor_num = int(valor)
                            df = df[df[col] == valor_num]
                        except:
                            df = df[df[col].astype(str) == valor]
                elif '>=' in self.filtro:
                    col, valor = self.filtro.split('>=')
                    col = col.strip()
                    valor = valor.strip()
                    
                    if col in df.columns:
                        try:
                            valor_num = int(valor)
                            df = df[df[col] >= valor_num]
                        except:
                            pass
                elif '<=' in self.filtro:
                    col, valor = self.filtro.split('<=')
                    col = col.strip()
                    valor = valor.strip()
                    
                    if col in df.columns:
                        try:
                            valor_num = int(valor)
                            df = df[df[col] <= valor_num]
                        except:
                            pass
                elif '>' in self.filtro:
                    col, valor = self.filtro.split('>')
                    col = col.strip()
                    valor = valor.strip()
                    
                    if col in df.columns:
                        try:
                            valor_num = int(valor)
                            df = df[df[col] > valor_num]
                        except:
                            pass
                elif '<' in self.filtro:
                    col, valor = self.filtro.split('<')
                    col = col.strip()
                    valor = valor.strip()
                    
                    if col in df.columns:
                        try:
                            valor_num = int(valor)
                            df = df[df[col] < valor_num]
                        except:
                            pass
                else:
                    # Fallback para query() se não reconhecer o operador
                    df = df.query(self.filtro)
            except Exception as e:
                self._write(f"   ⚠️ Erro ao aplicar filtro '{self.filtro}': {e}")
        
        return df
    
    def processar_em_chunks(self, destino='postgres'):
        """Processa o arquivo (CSV dentro de zip ou Parquet) em chunks com progresso"""
        
        self._write(f"📄 Processando em chunks: {self.nome_arquivo}")
        self._write(f"   ⚙️  Estratégia de conflito: {self.conflict_strategy}")
        # COPY em FORMAT csv dispensa limpeza; o mongo guarda o texto como veio
        self.limpar_texto = (destino == 'mongo')
        if self.filtro:
            self._write(f"   🔍 Filtro: {self.filtro}")
        
        try:
            tamanho_mb = os.path.getsize(self.arquivo_path) / (1024 * 1024)
            self._write(f"   📁 Tamanho do arquivo: {tamanho_mb:.1f} MB")
            
            if tamanho_mb > 500 and self.batch_size > 50000:
                self._write(f"   ℹ️ Arquivo grande ({tamanho_mb:.1f} MB). Batch {self.batch_size} requer boa memória RAM.")
        except:
            pass
        
        try:
            chunks, total_registros = self._iterar_chunks()
        except Exception as e:
            self._write(f"   ❌ Erro ao abrir arquivo: {e}")
            raise

        if total_registros:
            if self.test_limit > 0 and self.test_limit < total_registros:
                total_registros = self.test_limit
                self._write(f"   🧪 Teste limitado a {total_registros:,} registros")
            self._write(f"   📊 Total de registros: {total_registros:,}")
        else:
            total_registros = None
            self._write("   📊 Total de registros: desconhecido (CSV, contado durante a carga)")
        
        self._write(f"   📦 Tamanho do chunk: {self.batch_size:,}")
        self._write(f"   📈 Progresso a cada: {self.progress_interval:,} registros")
        if self.dump:
            self._write(f"   📝 Dump salvo automaticamente a cada {self.progress_interval:,} registros")
        self._write("")
        
        inicio = time.time()
        total_processado = 0
        chunk_count = 0
        erros = 0
        linhas_ignoradas = 0
        ultimo_progresso = 0
        
        # chunks: gerador de DataFrame (CSV dentro de zip ou arquivo Parquet)
        
        try:
            for df in chunks:
                if self.test_limit > 0 and total_processado >= self.test_limit:
                    break
                
                try:
                    chunk_count += 1
                    df = self._transformar_chunk(df)

                    if self.test_limit > 0:
                        sobra = self.test_limit - total_processado
                        if len(df) > sobra:
                            df = df.iloc[:sobra]
                    
                    # Se o filtro removeu todos os registros, pula o chunk
                    if len(df) == 0:
                        continue
                    
                    if destino == 'postgres':
                        ignoradas = self._carregar_chunk_postgres(df)
                        linhas_ignoradas += ignoradas
                    elif destino == 'mongo':
                        ignoradas = self._carregar_chunk_mongo(df)
                        linhas_ignoradas += ignoradas
                    
                    total_processado += len(df)
                    
                    if total_processado - ultimo_progresso >= self.progress_interval or (total_registros and total_processado >= total_registros):
                        tempo_decorrido = time.time() - inicio
                        velocidade = total_processado / tempo_decorrido if tempo_decorrido > 0 else 0
                        tempo_restante = (total_registros - total_processado) / velocidade if (total_registros and velocidade > 0) else 0
                        
                        pct = (total_processado / total_registros * 100) if total_registros else 0
                        tempo_str = str(timedelta(seconds=int(tempo_decorrido)))
                        tempo_restante_str = str(timedelta(seconds=int(tempo_restante))) if total_registros else 'n/d'
                        
                        alvo = f"{total_processado:,}/{total_registros:,} ({pct:.1f}%)" if total_registros else f"{total_processado:,}"
                        self._write(f"   📊 {alvo} | "
                                   f"⏱️  {tempo_str} | "
                                   f"🚀 {velocidade:,.0f} reg/s | "
                                   f"⏳ restante: {tempo_restante_str} | "
                                   f"Ignorados: {linhas_ignoradas:,}")
                        ultimo_progresso = total_processado
                    
                    if chunk_count % 5 == 0:
                        gc.collect()
                    
                    del df
                except Exception as e:
                    erros += 1
                    self._write(f"   ❌ Erro no chunk {chunk_count}: {e}")
                    if erros > 5:
                        raise Exception(f"Demasiados erros ({erros}) no arquivo {self.nome_arquivo}")
                    continue
            
            if self.dump and self.registros_pulados and len(self.registros_pulados) > 0:
                nome_arquivo = self._salvar_dump(self.registros_pulados, "_final")
                if nome_arquivo:
                    self._write(f"   💾 Dump final salvo: {nome_arquivo} ({len(self.registros_pulados):,} pulados)")
            
            self._write("")
            self._write(f"   ✅ Arquivo processado: {total_processado:,} registros, {linhas_ignoradas:,} ignorados")
            return total_processado
        except Exception as e:
            self._write(f"❌ Erro fatal no processamento: {e}")
            raise
        finally:
            try:
                chunks.close()
            except Exception:
                pass
            gc.collect()
    
    def _carregar_chunk_mongo(self, df):
        """Carrega um chunk no MongoDB usando chave composta como _id
        🔥 Datas convertidas para ISODate (datetime nativo)"""
        if len(df) == 0:
            return 0
        
        db = get_mongo_database()
        colecao = db[self.tabela_nome]
        
        docs = []
        pulados = 0
        
        for idx, row in df.iterrows():
            doc = {}
            
            if self.tabela_nome == 'c_estabelecimentos':
                cnpj_basico = str(row.get('cnpj_basico', '')).strip()
                cnpj_ordem = str(row.get('cnpj_ordem', '')).strip()
                cnpj_dv = str(row.get('cnpj_dv', '')).strip()
                
                if cnpj_basico and cnpj_basico != 'nan' and cnpj_basico != 'None':
                    doc['_id'] = f"{cnpj_basico}|{cnpj_ordem}|{cnpj_dv}"
                else:
                    doc['_id'] = f"row_{idx}"
            else:
                campo_id = self.campo_id
                if not campo_id:
                    for col in df.columns:
                        if col in ['id_empresa', 'id_socio', 'id_simples']:
                            campo_id = col
                            break
                
                if campo_id and campo_id in row and row[campo_id] is not None:
                    doc['_id'] = row[campo_id]
                elif isinstance(self.chave, list):
                    partes = []
                    for chave in self.chave:
                        if chave in row and row[chave] is not None:
                            partes.append(str(row[chave]))
                        else:
                            partes.append("NULL")
                    doc['_id'] = "|".join(partes)
                elif self.chave and self.chave in row and row[self.chave] is not None:
                    doc['_id'] = row[self.chave]
                else:
                    doc['_id'] = f"row_{idx}"
            
            # 🔥 Adicionar todos os campos ao documento
            for col, val in row.items():
                if pd.isna(val):
                    doc[col] = None
                elif isinstance(val, pd.Timestamp):
                    # 🔥 CONVERTER PARA datetime NATIVO (MongoDB armazena como ISODate)
                    doc[col] = val.to_pydatetime()
                else:
                    doc[col] = val
            
            docs.append(doc)
        
        try:
            if docs:
                try:
                    resultado = colecao.insert_many(docs, ordered=False)
                except Exception as e:
                    pulados = 0
                    for doc in docs:
                        try:
                            colecao.insert_one(doc)
                        except Exception as e2:
                            pulados += 1
                    return pulados
                
                if hasattr(resultado, 'bulk_api_result') and resultado.bulk_api_result:
                    if 'writeErrors' in resultado.bulk_api_result:
                        for erro in resultado.bulk_api_result['writeErrors']:
                            pulados += 1
        except Exception as e:
            logger.error(f"Erro no insert_many: {e}")
            for doc in docs:
                try:
                    colecao.insert_one(doc)
                except Exception as e2:
                    pulados += 1
        
        return pulados
    
    # COPY em CSV: tab/quebra/aspas dentro dos campos sao citados pelo pandas,
    # entao nao precisa limpar o texto antes (era o gargalo dos 4 mil lin/s).
    # NULL '': campo vazio (inclusive string vazia) vira NULL, como antes.
    CSV_COPY = "WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE '\"', ESCAPE '\"')"

    def _copy_sql(self, tabela, colunas):
        return f"COPY {tabela} ({', '.join(colunas)}) FROM STDIN {self.CSV_COPY}"

    def _buffer_tsv(self, df, colunas):
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False, na_rep='',
                  quoting=csv.QUOTE_MINIMAL, columns=colunas)
        output.seek(0)
        return output

    def _carregar_chunk_postgres(self, df):
        """Carrega um chunk no PostgreSQL usando COPY (FORMAT csv)"""
        if len(df) == 0:
            return 0

        conn = get_postgres_connection()
        cur = conn.cursor()
        linhas_ignoradas = 0
        df_filtrado = df

        try:
            # FORCAR A ORDEM DAS COLUNAS (remover IDs artificiais)
            colunas_ignorar = ['id_empresa', 'id_socio', 'id_estabelecimento', 'id_simples']
            colunas = [col for col in self.colunas if col not in colunas_ignorar]
            df_filtrado = df[colunas]

            if self.conflict_strategy == 'none':
                output = self._buffer_tsv(df_filtrado, colunas)
                try:
                    cur.copy_expert(self._copy_sql(self.tabela_nome, colunas), output)
                    conn.commit()
                    self._write(f"   \u26a1 COPY: {len(df)} registros")
                except Exception as e:
                    conn.rollback()
                    self._write(f"   \u26a0\ufe0f COPY falhou: {str(e)[:100]}... Usando INSERT...")
                    linhas_ignoradas = self._carregar_chunk_insert_simples(df_filtrado, conn, cur)
                return linhas_ignoradas

            temp_table = f"temp_{self.tabela_nome}_{id(df)}_{os.getpid()}"
            cur.execute(f"""
                CREATE TEMP TABLE {temp_table} (
                    LIKE {self.tabela_nome} INCLUDING DEFAULTS
                ) ON COMMIT DROP
            """)
            output = self._buffer_tsv(df_filtrado, colunas)
            cur.copy_expert(self._copy_sql(temp_table, colunas), output)

            colunas_str = ', '.join(colunas)
            if isinstance(self.chave, list):
                conflito = ', '.join([k for k in self.chave if k in colunas])
                update_cols = [col for col in colunas if col not in self.chave]
            else:
                conflito = self.chave if self.chave in colunas else colunas[0]
                update_cols = [col for col in colunas if col != conflito]

            update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])

            if self.conflict_strategy == 'update':
                sql = f"""
                    INSERT INTO {self.tabela_nome} ({colunas_str})
                    SELECT {colunas_str} FROM {temp_table}
                    ON CONFLICT ({conflito}) DO UPDATE SET
                        {update_str}
                """
                self._write(f"   \U0001f504 COPY + ON CONFLICT UPDATE: {len(df)} registros")
            else:
                sql = f"""
                    INSERT INTO {self.tabela_nome} ({colunas_str})
                    SELECT {colunas_str} FROM {temp_table}
                    ON CONFLICT ({conflito}) DO NOTHING
                """
                self._write(f"   \u23ed\ufe0f COPY + ON CONFLICT NOTHING: {len(df)} registros")

            cur.execute(sql)
            conn.commit()

        except Exception as e:
            conn.rollback()
            self._write(f"   \u26a0\ufe0f COPY falhou: {str(e)[:100]}... Usando INSERT...")
            linhas_ignoradas = self._carregar_chunk_insert_simples(df_filtrado, conn, cur)

        finally:
            cur.close()
            conn.close()

        return linhas_ignoradas

    def _carregar_chunk_insert_simples(self, df, conn, cur):
        """Fallback: INSERT simples (sem ON CONFLICT)"""
        colunas = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(colunas))
        colunas_str = ', '.join(colunas)
        
        sql = f"""
            INSERT INTO {self.tabela_nome} ({colunas_str})
            VALUES ({placeholders})
        """
        
        dados = [tuple(None if pd.isna(v) else v for v in row) for _, row in df.iterrows()]
        linhas_ignoradas = 0
        
        try:
            cur.executemany(sql, dados)
            conn.commit()
        except Exception as e_batch:
            conn.rollback()
            for i, linha in enumerate(dados):
                try:
                    cur.execute(sql, linha)
                    conn.commit()
                except Exception as e_linha:
                    linhas_ignoradas += 1
                    conn.rollback()
                    continue
        
        return linhas_ignoradas
    
    def processar(self, destino='postgres'):
        """Pipeline completo de processamento"""
        try:
            return self.processar_em_chunks(destino)
        except Exception as e:
            print(f"❌ Erro em {self.nome_arquivo}: {e}")
            raise