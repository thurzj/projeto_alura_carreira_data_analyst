"""
Projeto: Análise de Vendas
Autor: Arthur
Descrição:
Este script realiza a leitura, tratamento, análise e visualização de dados de vendas,
além de carregar os dados tratados em um banco de dados MySQL.
"""

# ===============================
# IMPORTAÇÕES
# ===============================

import os
import urllib.parse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dotenv import load_dotenv
from sqlalchemy import create_engine

# ===============================
# CARREGAMENTO DAS VARIÁVEIS DE AMBIENTE
# ===============================

# Carrega as credenciais do arquivo .env
load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Codifica a senha para permitir caracteres especiais (@, !, etc)
password_encoded = urllib.parse.quote_plus(password)

# Cria a conexão com o banco de dados MySQL
engine = create_engine(
    f"mysql+pymysql://{user}:{password_encoded}@{host}:{port}/{db_name}"
)

# ===============================
# LEITURA DOS DADOS
# ===============================

# Carrega os dados de vendas e clientes
df_vendas = pd.read_csv('vendas_alura.csv', encoding='utf-8')
df_clientes = pd.read_csv('clientes.csv', encoding='utf-8')

# ===============================
# TRATAMENTO DE DADOS
# ===============================

# Remove acentos e caracteres especiais dos emails
df_clientes['email'] = (
    df_clientes['email']
    .str.normalize('NFKD')
    .str.encode('ascii', errors='ignore')
    .str.decode('utf-8')
)

# Converte a coluna de data para formato datetime
df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])

# Cria a coluna de faturamento total por venda
df_vendas['faturamento_total'] = (
    df_vendas['quantidade'] * df_vendas['valor_venda']
)

# ===============================
# ANÁLISES
# ===============================

# Calcula o faturamento mensal
faturamento_mensal = (
    df_vendas
    .groupby(pd.Grouper(key='data_venda', freq='MS'))['faturamento_total']
    .sum()
    .reset_index()
)

# Calcula o faturamento total por categoria
faturamento_categoria = (
    df_vendas
    .groupby('categoria')['faturamento_total']
    .sum()
    .reset_index()
    .sort_values(by='faturamento_total', ascending=False)
)

# ===============================
# VISUALIZAÇÃO DOS DADOS
# ===============================

sns.set_theme(style='whitegrid')

fig, ax = plt.subplots(1, 2, figsize=(14, 5))

# Gráfico de faturamento mensal
sns.lineplot(
    data=faturamento_mensal,
    x='data_venda',
    y='faturamento_total',
    marker='o',
    ax=ax[0]
)

ax[0].set_title('Faturamento Total por Mês')
ax[0].set_xlabel('Mês')
ax[0].set_ylabel('Faturamento')
ax[0].xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%b/%Y'))
plt.setp(ax[0].get_xticklabels(), rotation=45)

# Gráfico de faturamento por categoria
sns.barplot(
    data=faturamento_categoria,
    x='categoria',
    y='faturamento_total',
    ax=ax[1]
)

ax[1].set_title('Faturamento por Categoria')
ax[1].set_xlabel('Categoria')
ax[1].set_ylabel('Faturamento')
plt.setp(ax[1].get_xticklabels(), rotation=45)

plt.tight_layout()
plt.show()

# ===============================
# EXPORTAÇÃO DOS DADOS TRATADOS
# ===============================

# Salva os dados tratados em CSV
df_clientes.to_csv('clientes_tratados.csv', index=False, encoding='utf-8')
df_vendas.to_csv('vendas_tratadas.csv', index=False, encoding='utf-8')

# ===============================
# CARGA NO BANCO DE DADOS
# ===============================

# Envia os dados tratados para o MySQL
df_clientes.to_sql('clientes', con=engine, if_exists='replace', index=False)
df_vendas.to_sql('vendas', con=engine, if_exists='replace', index=False)

print("Pipeline de dados executado com sucesso.")
