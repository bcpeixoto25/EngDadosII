import pandas as pd
import duckdb
import pandera.pandas as pa


def csv_bronze(csv_file):
    """
    Vai consumir o CSV, apagar a tabela na camada bronze e a recriar
     com os dados do CSV
    """
    link_csv = csv_file
    print(f'Consumindo arquivo {csv_file}...')
    df_ingestao = pd.read_csv(link_csv)

    # CONVERTENDO PARA PARQUET
    print(f'Convertendo arquivo {csv_file} para parquet...')
    df_ingestao.to_parquet('desafio.parquet',
                           engine='pyarrow',
                           index=False)

    # Conectando ao banco de dados (ou criando um novo)
    print('Conectando ao banco de dados duckdb...')
    conn = duckdb.connect("banco_desafio.duckdb")

    # Criando SCHEMA
    print('Criando SCHEMA bronze...')
    conn.execute("""CREATE SCHEMA IF NOT EXISTS bronze;
                """)
    # Apagando a tabela, se existe
    print('Apagando tabela, se existir...')
    conn.execute("DROP TABLE IF EXISTS bronze.tab_desafio;")
    # Criando a tabela a partir do arquivo parquet
    print('Criando a tabela a partir do arquivo parquet...')
    conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze.tab_desafio
                  AS SELECT * FROM 
                 read_parquet('desafio.parquet');
                """)

    # Consultando
    print('Consultando dados sonstantes na tabela bronze...')
    df_result = conn.execute("select * from bronze.tab_desafio").fetchdf()

    print(f'Dados obtidos:\n {df_result}')
    # Fechar a conexão
    conn.close()
    print('Conexão fechada com dados salvos...')
    return df_result


def modificar(df: pd.DataFrame):
    """
    Modifica um dataframe trocando o lugar da coluna "idade" por "apelido"
     e adiciona uma coluna "flag" com zero
    """
    df_modificado = df[['nome', 'apelido', 'idade']]
    df_modificado['flag'] = 0
    print('Dados transformados...')
    return df_modificado


def verifica_df(df: pd.DataFrame):
    # regra de validação
    schema = pa.DataFrameSchema(
        {
            "nome": pa.Column(str),
            "apelido": pa.Column(str),
            "idade": pa.Column(int),
            "flag": pa.Column(int),
        }
    )
    try:
        schema.validate(df)
        print('Dados validados e prontos...')
        return True
    except pa.errors.SchemaError as exc:
        print(f'Xiii... Algo deu errado ao validar dados: {exc}')
        return False


def create_schema_silver():
    con = duckdb.connect("banco_desafio.duckdb")
    print('Criando SCHEMA silver, se necessário...')
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.close()


def create_silver_tab_desafio(df: pd.DataFrame):
    con = duckdb.connect("banco_desafio.duckdb")
    con.execute("CREATE TABLE IF NOT EXISTS silver.tab_desafio AS SELECT * FROM df")
    print('Tabela nodificada salva na camada silver com sucesso. VALE NOTA 10!')
    con.close()
