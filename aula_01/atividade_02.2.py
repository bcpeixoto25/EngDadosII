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