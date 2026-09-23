import os
import psycopg2
from django.conf import settings
from datetime import datetime

class ControleArquivos:
    """Gerencia o controle de arquivos já processados usando PostgreSQL diretamente"""
    
    @staticmethod
    def _get_connection():
        """Obtém conexão com PostgreSQL"""
        return psycopg2.connect(
            dbname=settings.PG_DB,
            user=settings.PG_USER,
            password=settings.PG_PASSWORD,
            host=settings.PG_HOST,
            port=settings.PG_PORT
        )
    
    @staticmethod
    def ja_processado(caminho_arquivo, banco_destino, tabela):
        """Verifica se o arquivo já foi processado"""
        nome_arquivo = os.path.basename(caminho_arquivo)
        
        conn = ControleArquivos._get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM etl_arquivoprocessado 
            WHERE nome_arquivo = %s 
            AND banco_destino = %s 
            AND tabela = %s 
            AND status = 'SUCESSO'
        """, (nome_arquivo, banco_destino, tabela))
        
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        return count > 0
    
    @staticmethod
    def registrar_processamento(caminho_arquivo, banco_destino, tabela, total_registros, status='SUCESSO'):
        """Registra ou atualiza o processamento de um arquivo"""
        nome_arquivo = os.path.basename(caminho_arquivo)
        
        conn = ControleArquivos._get_connection()
        cur = conn.cursor()
        
        try:
            # Tenta inserir
            cur.execute("""
                INSERT INTO etl_arquivoprocessado 
                (nome_arquivo, caminho_completo, banco_destino, tabela, registros_importados, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (nome_arquivo, caminho_arquivo, banco_destino, tabela, total_registros, status))
        except psycopg2.errors.UniqueViolation:
            # Se já existe, atualiza
            conn.rollback()
            cur.execute("""
                UPDATE etl_arquivoprocessado 
                SET caminho_completo = %s, 
                    registros_importados = %s, 
                    status = %s,
                    data_processamento = CURRENT_TIMESTAMP
                WHERE nome_arquivo = %s 
                AND banco_destino = %s 
                AND tabela = %s
            """, (caminho_arquivo, total_registros, status, nome_arquivo, banco_destino, tabela))
        
        conn.commit()
        cur.close()
        conn.close()
    
    @staticmethod
    def listar_processados():
        """Lista todos os arquivos já processados"""
        conn = ControleArquivos._get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT nome_arquivo, banco_destino, tabela, registros_importados, 
                   data_processamento, status
            FROM etl_arquivoprocessado
            ORDER BY data_processamento DESC
        """)
        
        resultados = cur.fetchall()
        cur.close()
        conn.close()
        
        return resultados
    
    @staticmethod
    def limpar_registro(nome_arquivo, banco_destino=None, tabela=None):
        """Remove registros de controle (útil para forçar reprocessamento)"""
        conn = ControleArquivos._get_connection()
        cur = conn.cursor()
        
        sql = "DELETE FROM etl_arquivoprocessado WHERE nome_arquivo = %s"
        params = [nome_arquivo]
        
        if banco_destino:
            sql += " AND banco_destino = %s"
            params.append(banco_destino)
        
        if tabela:
            sql += " AND tabela = %s"
            params.append(tabela)
        
        cur.execute(sql, params)
        conn.commit()
        
        deleted = cur.rowcount
        cur.close()
        conn.close()
        
        return deleted