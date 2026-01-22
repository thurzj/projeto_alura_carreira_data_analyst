import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import csv
import os
from dotenv import load_dotenv

import urllib.parse  # Adicione este import no topo
from sqlalchemy import create_engine

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

password_encoded = urllib.parse.quote_plus(password)

engine = create_engine(
    f"mysql+pymysql://{user}:{password_encoded}@{host}:{port}/{db_name}"
)
df_vendas_tratadas = pd.read_csv('vendas_alura.csv', encoding = 'utf-8')
df_clientes_tratados = pd.read_csv('clientes.csv', encoding = 'utf-8')

df_clientes_tratados['email'] = (
    df_clientes_tratados['email']
    .str.normalize('NFKD')
    .str.encode('ascii', errors='ignore')
    .str.decode('utf-8')
)


df_vendas_tratadas['data_venda'] = pd.to_datetime(df_vendas_tratadas['data_venda'])


df_vendas_tratadas['faturamento_total'] = df_vendas_tratadas['quantidade']* df_vendas_tratadas['valor_venda']

tendencia = df_vendas_tratadas.groupby(pd.Grouper(key = 'data_venda', freq = 'MS'))['faturamento_total'].sum().reset_index()

agrupmento_categoria = df_vendas_tratadas.groupby('categoria')['faturamento_total'].sum().reset_index()

agrupmento_categoria = agrupmento_categoria.sort_values(
    by='faturamento_total',
    ascending=False
)

sns.set_theme(style = 'whitegrid')

fig, ax = plt.subplots(1,2, figsize=(14,5))

sns.lineplot(
        data = tendencia,
        x = 'data_venda',
        y = 'faturamento_total',
        marker = 'o',
        ax = ax[0]
)

ax[0].set_title('Faturamento Total por Mês')
ax[0].set_xlabel('Mês')
ax[0].set_ylabel('Faturamento Total')
ax[0].xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%b/%Y'))
plt.setp(ax[0].get_xticklabels(), rotation=45, ha='right')


sns.barplot(
        data = agrupmento_categoria,
        x = 'categoria',
        y = 'faturamento_total',
        ax = ax[1]
)

ax[1].set_title('Faturamento por categoria')
ax[1].set_xlabel('Categoria')
ax[1].set_ylabel('Faturamento Total')
plt.setp(ax[1].get_xticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.show()


df_clientes_tratados.to_csv('clientes_tratados.csv', 
                           index=False, 
                           encoding='utf-8')
                           
df_vendas_tratadas.to_csv('vendas_tratadas.csv', 
                         index=False, 
                         encoding='utf-8')


df_clientes_tratados.to_sql('clientes', con=engine, if_exists='replace', index=False)
df_vendas_tratadas.to_sql('vendas', con=engine, if_exists='replace', index=False)