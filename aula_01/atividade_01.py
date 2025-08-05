"""
Atividade 1) Warm-up: mini “lakehouse” local (30-40 min)
Objetivo: ingestir → transformar → consultar em DuckDB.
"""
import pandas as pd

#CONSUMINDO CSV
link_csv = "https://drive.google.com/uc?export=download&id=1_bzOeXtDmlOnSLoW7IpLT_vNWxvJ-VJB"
df_ingestao = pd.read_csv(link_csv)

#CONVERTENDO PARA PARQUET
#df_ingestao.to_parquet('arquivo.parquet', index = False)
df_ingestao.to_parquet('arquivo.parquet', engine='pyarrow', index = False)

#Crie tabela "bronze" no DuckDB e rode uma query
import duckdb

# Conectando ao banco de dados (ou criando um novo)
conn = duckdb.connect("meu_banco_duckdb.duckdb")

#Criando SCHEMA
conn.execute("""CREATE SCHEMA IF NOT EXISTS bronze;
             """)

# Criando a tabela a aprtir do arquivo parquet
conn.execute("""CREATE TABLE IF NOT EXISTS bronze.avaliacoes AS SELECT * FROM read_parquet('arquivo.parquet');
             """)

#Consultando
df_result = conn.execute("select * from bronze.avaliacoes LIMIT 10").fetchdf()

print(df_result)
print(type(df_result))

# 5. Fechar a conexão
conn.close()