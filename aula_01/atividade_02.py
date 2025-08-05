"""
Atividade 2.1) Pipeline-as-Code (Prefect-like) sem servidor (35-45 min)
Objetivo: orquestrar funções com retries e parâmetros (sem UI).  
1. Crie um pipeline usando o Prefect para ler os dados da camada bronze e criar a tabela
na camada silver.
"""

from prefect import flow, task
import duckdb
import pandas as pd

@task
def fetch_bronze_data() -> pd.DataFrame:
    con = duckdb.connect("meu_banco_duckdb.duckdb")
    query = "SELECT * FROM bronze.avaliacoes"
    df = con.execute(query).df()
    con.close()
    return df

@task
def create_schema_silver():
    con = duckdb.connect("meu_banco_duckdb.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.close()

@task
def create_silver_avaliacoes_table(df: pd.DataFrame):
    con = duckdb.connect("meu_banco_duckdb.duckdb")
    con.execute("CREATE TABLE IF NOT EXISTS silver.avaliacoes AS SELECT * FROM df")
    con.close()

@flow
def main():
    df = fetch_bronze_data()
    create_schema_silver()
    create_silver_avaliacoes_table(df)

if __name__ == "__main__":
    # main.serve(
    #     name="Ingestão de Dados Bronze para Silver",
    #     cron="1 * * * *",
    # )
    main()
