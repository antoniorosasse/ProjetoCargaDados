import pandas as pd
import sys
import glob
import os
import re

def extrair_numero(nome_arquivo):
    """Extrai o número do nome do arquivo para ordenação"""
    match = re.search(r'(\d+)\.parquet$', nome_arquivo, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 0

def analisar_arquivo(arquivo):
    """Analisa um arquivo Parquet e retorna um resumo"""
    try:
        df = pd.read_parquet(arquivo)
        return {
            'arquivo': arquivo,
            'registros': len(df),
            'colunas': len(df.columns),
            'colunas_nomes': df.columns.tolist(),
            'tipos': df.dtypes.to_dict(),
            'amostra': df.head(3),
            'nulos': df.isnull().sum().to_dict(),
            'df': df
        }
    except Exception as e:
        print(f"❌ Erro ao ler {arquivo}: {e}")
        return None

def main():
    if len(sys.argv) < 2:
        print("Uso: python ver_parquet_sequencia.py <padrao_arquivo>")
        print("Exemplo: python ver_parquet_sequencia.py 'empresas*.parquet'")
        print("Exemplo: python ver_parquet_sequencia.py '/home/neto/DataSetsTeste/base_cnpj/Empresas*.parquet'")
        sys.exit(1)

    padrao = sys.argv[1]
    
    # Listar todos os arquivos que correspondem ao padrão
    arquivos = glob.glob(padrao)
    
    if not arquivos:
        print(f"❌ Nenhum arquivo encontrado com o padrão: {padrao}")
        sys.exit(1)
    
    # Ordenar arquivos numericamente
    arquivos.sort(key=lambda x: extrair_numero(os.path.basename(x)))
    
    print("=" * 80)
    print(f"📁 Encontrados {len(arquivos)} arquivo(s) com o padrão: {padrao}")
    print("=" * 80)
    
    # Analisar cada arquivo
    resultados = []
    total_registros = 0
    total_colunas = set()
    
    for i, arquivo in enumerate(arquivos, 1):
        print(f"\n📄 [{i}/{len(arquivos)}] Analisando: {os.path.basename(arquivo)}")
        print("-" * 60)
        
        resultado = analisar_arquivo(arquivo)
        if resultado:
            resultados.append(resultado)
            total_registros += resultado['registros']
            total_colunas.update(resultado['colunas_nomes'])
            print(f"   📊 Registros: {resultado['registros']:,}")
            print(f"   📋 Colunas: {resultado['colunas']}")
            print(f"   📝 Primeiras colunas: {', '.join(resultado['colunas_nomes'][:5])}...")
    
    # Resumo geral
    print("\n" + "=" * 80)
    print("📊 RESUMO GERAL - TODOS OS ARQUIVOS")
    print("=" * 80)
    print(f"📁 Total de arquivos processados: {len(resultados)}")
    print(f"📊 TOTAL DE REGISTROS SOMADOS: {total_registros:,}")
    print(f"📋 Total de colunas únicas: {len(total_colunas)}")
    
    # Verificar consistência das colunas
    if resultados:
        primeira_colunas = set(resultados[0]['colunas_nomes'])
        inconsistentes = []
        for i, res in enumerate(resultados[1:], 2):
            if set(res['colunas_nomes']) != primeira_colunas:
                inconsistentes.append(i)
        
        if inconsistentes:
            print(f"⚠️  Arquivos com colunas diferentes: {inconsistentes}")
        else:
            print("✅ Todos os arquivos têm as mesmas colunas")
    
    # Mostrar amostra do primeiro arquivo
    if resultados:
        print("\n📝 AMOSTRA DO PRIMEIRO ARQUIVO:")
        print("-" * 60)
        print(resultados[0]['df'].head(5).to_string())
    
    # ============================================================
    # PERGUNTAR SE QUER CONCATENAR (NÃO FAZ AUTOMATICAMENTE)
    # ============================================================
    
    print("\n" + "=" * 80)
    resposta = input("🔄 Deseja concatenar todos os arquivos em um único DataFrame? (s/N): ")
    
    if resposta.lower() == 's':
        print("\n" + "=" * 80)
        print("📊 CONCATENANDO TODOS OS ARQUIVOS...")
        print("=" * 80)
        
        try:
            # Coletar todos os DataFrames
            todos_dfs = [res['df'] for res in resultados]
            
            # Concatenar
            df_consolidado = pd.concat(todos_dfs, ignore_index=True)
            print(f"✅ DataFrames concatenados com sucesso!")
            print(f"📊 Total de registros no DataFrame consolidado: {len(df_consolidado):,}")
            print(f"📋 Total de colunas: {len(df_consolidado.columns)}")
            
            # Mostrar amostra do consolidado
            print("\n📝 AMOSTRA DO CONSOLIDADO (primeiras 5 linhas):")
            print("-" * 60)
            print(df_consolidado.head(5).to_string())
            
            # Estatísticas básicas do consolidado
            print("\n📊 ESTATÍSTICAS DO CONSOLIDADO:")
            print("-" * 60)
            nulos = df_consolidado.isnull().sum()
            for col, val in nulos.items():
                if val > 0:
                    print(f"  {col}: {val:,} ({val/len(df_consolidado)*100:.1f}%)")
            
            # Perguntar se quer salvar
            print("\n" + "=" * 80)
            resposta_salvar = input("💾 Deseja salvar o DataFrame consolidado? (s/N): ")
            if resposta_salvar.lower() == 's':
                print("\n📁 Formatos disponíveis:")
                print("  1. Parquet (recomendado - menor, mais rápido)")
                print("  2. CSV (maior, mas compatível com Excel)")
                print("  3. Ambos")
                
                formato = input("\nEscolha o formato (1/2/3): ")
                
                nome_base = f"consolidado_{os.path.basename(os.path.dirname(padrao))}"
                if not nome_base or nome_base == '.':
                    nome_base = "consolidado"
                
                if formato == '1' or formato == '3':
                    nome_parquet = f"{nome_base}.parquet"
                    df_consolidado.to_parquet(nome_parquet, index=False)
                    print(f"✅ Parquet salvo como: {nome_parquet}")
                    print(f"   Tamanho: {os.path.getsize(nome_parquet) / (1024*1024):.1f} MB")
                
                if formato == '2' or formato == '3':
                    nome_csv = f"{nome_base}.csv"
                    df_consolidado.to_csv(nome_csv, index=False)
                    print(f"✅ CSV salvo como: {nome_csv}")
                    if os.path.exists(nome_csv):
                        print(f"   Tamanho: {os.path.getsize(nome_csv) / (1024*1024):.1f} MB")
                
                print("\n✅ Arquivo(s) salvos com sucesso!")
                
        except Exception as e:
            print(f"❌ Erro ao concatenar: {e}")
            print("   Os DataFrames podem ter colunas diferentes ou tipos incompatíveis.")
    else:
        print("\n⏭️  Concatenação pulada (você pode rodar novamente se quiser).")
    
    # Perguntar se quer ver mais detalhes
    print("\n" + "=" * 80)
    resposta = input("🔍 Deseja ver estatísticas detalhadas de um arquivo específico? (s/N): ")
    if resposta.lower() == 's':
        print("\nArquivos disponíveis:")
        for i, res in enumerate(resultados, 1):
            print(f"  {i:3}. {os.path.basename(res['arquivo'])} ({res['registros']:,} registros)")
        
        try:
            idx = int(input("\nDigite o número do arquivo: ")) - 1
            if 0 <= idx < len(resultados):
                res = resultados[idx]
                print("\n" + "=" * 60)
                print(f"📄 ANALISE DETALHADA: {os.path.basename(res['arquivo'])}")
                print("=" * 60)
                print(f"\n📋 Colunas ({res['colunas']}):")
                for i, col in enumerate(res['colunas_nomes'], 1):
                    print(f"  {i:3}. {col} ({res['tipos'][col]})")
                
                print("\n📊 Registros nulos por coluna:")
                for col, nulos in res['nulos'].items():
                    if nulos > 0:
                        print(f"  {col}: {nulos:,} ({nulos/res['registros']*100:.1f}%)")
                
                print("\n📝 Primeiras 3 linhas:")
                print(res['df'].head(3).to_string())
        except:
            print("❌ Número inválido.")

if __name__ == "__main__":
    main()