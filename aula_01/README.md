# AULA 01 - 02.08.2023
- A aula foi composta de 3 atividades e um desafio.

## ATIVIDADE 01
* Warm-up: mini “lakehouse” local (30-40 min)

* Objetivo: ingestir → transformar → consultar em DuckDB.

```python
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
```

## ATIVIDADE 02
* Pipeline-as-Code (Prefect-like) sem servidor (35-45 min)
* Objetivo: orquestrar funções com retries e parâmetros (sem UI).  
1. Crie um pipeline usando o Prefect para ler os dados da camada bronze e criar a tabela
na camada silver.
```python
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
```

## ATIVIDADE 02.2
* Validação de dados
* Adicione uma etapa para fazer validação de dados com Pandera (25-35 min)
* https://pandera.readthedocs.io/en/stable/dataframe_schemas.html
```python
"""
Atividade 2.2) Validação de dados
Adicione uma etapa para fazer validação de dados com Pandera (25-35 min)
https://pandera.readthedocs.io/en/stable/dataframe_schemas.html
"""

import pandas as pd
import pandera.pandas as pa
import duckdb

#regra de validação
schema = pa.DataFrameSchema(
    {
        "discipline": pa.Column(str),
        "workload_hours": pa.Column(int),
        "final_grade": pa.Column(float),
    }
)

#Obtendo dados da tabela avaliacoes da camada silver
con = duckdb.connect("meu_banco_duckdb.duckdb")
query = "SELECT * FROM silver.avaliacoes"
df = con.execute(query).df()
con.close()


try:
    schema.validate(df)
    print('Tudo certo!')
except pa.errors.SchemaError as exc:
    print(f'Xiii... Algo deu errado: {exc}')
```

## DESAFIO
* “Sensor” de arquivo com watchdog
* *Objetivo*: rodar o pipeline quando chegar um CSV novo em incoming. 

### Contexto
- Uma pasta será monitorada e toda vez um arquivo `.cvs` for adicionado ou alterado, os dados serão transferidos para a camada bronze, tratados com a inclusão de uma coluna e, em seguida, salvos na camada silver.
- a estrutura do cvs deve ser:

| nome     | idade | apelido        |
|----------|-------|----------------|
| Joaquim  | 15    | Quimquim       |
| Jose     | 25    | Olho de Tandera|
| Ambrosio | 35    | Chicago        |

### Estrutura
- será criado um arquivo com as funções do pipeline (`desafio.py`) e outro para o monitoramento da pasta (`monitor.py`).

### Arquivo `desafio.py`
- As bibliotecas serão a pandas para o tratamento de dados, duckdb e pandera, estas últimas utilização quando da manipulação das bases em duckdb e parquet

```python
import pandas as pd
import duckdb
import pandera.pandas as pa
```
- A primeira função importa o arquivo csv da pasta para um dataframe pandas. O dataframe será convertido para parquet e salvo na camada bronze na tabela `tab_desafio`.
```python
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
```

- A segunda função simula alteração dos dados, recebendo os dados em um dataframe, mudando a orde das colunas, criando uma coluna chamada `flag` com o valor zero, e devolvendo os dados como dataframe pandas.
```python
def modificar(df: pd.DataFrame):
    """
    Modifica um dataframe trocando o lugar da coluna "idade" por "apelido"
     e adiciona uma coluna "flag" com zero
    """
    df_modificado = df[['nome', 'apelido', 'idade']]
    df_modificado['flag'] = 0
    print('Dados transformados...')
    return df_modificado
```
- Por sua vez, a terceira função faz a validação dos dados.
```python
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
```
- Uma função para criar o schema silver.
```python
def create_schema_silver():
    con = duckdb.connect("banco_desafio.duckdb")
    print('Criando SCHEMA silver, se necessário...')
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.close()
```
- E, por último, uma função para criar a tabela no schema silver a aprtir de um dataframe fornecido.
```python
def create_silver_tab_desafio(df: pd.DataFrame):
    con = duckdb.connect("banco_desafio.duckdb")
    con.execute("CREATE TABLE IF NOT EXISTS silver.tab_desafio AS SELECT * FROM df")
    print('Tabela nodificada salva na camada silver com sucesso. VALE NOTA 10!')
    con.close()
```

### Arquivo `monitor.py`
- O monitoramento da pasta será feito com o modulo `watchdog` e o arquivo `monitor.py` será utilizado para monitorar a pasta e, em caso de alteração, executar o pipeline.
- O pipeline, por sua vez, irá consumir o CSV, transformar os dados, validar os dados e, em seguida, salvar os dados na camada silver.
- Bibliotecas utilizadas:
```python
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import desafio
``` 
- Vamos monitorar a criação, alteração e exclusão de arquivos na pasta definida, sendo que o pipeline somente irá ser executado nos dois primeiros caso. Para a deleção de arquivo, só a mensagem da ocorrência aparecerá, sem executar o pipeline.
```python
class WatcherHandler(FileSystemEventHandler):
    def fluxo(self, src_path):
        df = desafio.csv_bronze(src_path)
        df_modificado = desafio.modificar(df)
        if desafio.verifica_df(df_modificado):
            desafio.create_schema_silver()
            desafio.create_silver_tab_desafio(df_modificado)
        else:
            print('Verifique fluxo! Não deu certo!')

    def on_modified(self, event):
        print(f"Arquivo modificado: {event.src_path}")
        extensao = event.src_path.split('.')[-1]
        print(f'A extensão é {extensao}')
        if extensao == 'csv':
            print(f"Iniciando ingestão de dados com o arquivo:{event.src_path}")
            self.fluxo(event.src_path)
        else:
            print('Nada a fazer. O arquivo modificado não é .csv')   

    def on_created(self, event):
        print(f"Arquivo criado: {event.src_path}")
        extensao = event.src_path.split('.')[-1]
        print(f'A extensão é {extensao}')
        if extensao == 'csv':
            print(f"Iniciando ingestão de dados com o arquivo: {event.src_path}")
            self.fluxo(event.src_path)
        else:
            print('Nada a fazer. O arquivo modificado não é .csv')

    def on_deleted(self, event):
        print(f"Arquivo deletado: {event.src_path}")
        print('Nada a fazer')


if __name__ == "__main__":
    path_to_watch = ".\pasta"  # Directory to monitor
    event_handler = WatcherHandler()
    observer = Observer()
    observer.schedule(event_handler, path=path_to_watch, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
```
*FIM!*