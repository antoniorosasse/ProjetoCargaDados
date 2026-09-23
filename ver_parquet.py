import pandas as pd
import sys

if len(sys.argv) < 2:
    print("Uso: python ver_parquet.py <arquivo.parquet>")
    sys.exit(1)

df = pd.read_parquet(sys.argv[1])
print("=" * 60)
print(f"Arquivo: {sys.argv[1]}")
print("=" * 60)
print(f"\n📊 Total de registros: {len(df)}")
print(f"\n📋 Colunas ({len(df.columns)}):")
print("-" * 40)
for i, col in enumerate(df.columns, 1):
    print(f"{i:3}. {col} ({df[col].dtype})")

print("\n📝 Primeiras 3 linhas:")
print("-" * 40)
print(df.head(3).to_string())

print("\n📊 Estatísticas básicas:")
print("-" * 40)
print(f"Registros nulos por coluna:")
print(df.isnull().sum())
