from fastapi import FastAPI, Query
from typing import Optional
import pandas as pd
import pyodbc
from datetime import datetime

app = FastAPI()

# Conexão com o banco
def carregar_dados():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=aquidaba.infonet.com.br;"
        "DATABASE=dbproinfo;"
        "UID=leituraVendas;"
        "PWD=KRphDP65BM"
    )
    conn = pyodbc.connect(conn_str)
    query = "SELECT * FROM tbVendasDashboard"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Formatar datas da coluna
def preparar_dados(df):
    df["dtVenda"] = pd.to_datetime(df["dtVenda"])
    return df

@app.get("/vendas-dia-anterior")
def vendas_dia_anterior(
    filial: Optional[str] = Query(default=None, description="Nome da filial")
):
    df = preparar_dados(carregar_dados())
    ontem = (datetime.today() - pd.Timedelta(days=1)).date()
    vendas_ontem = df[df["dtVenda"].dt.date == ontem]
    if filial:
        vendas_ontem = vendas_ontem[vendas_ontem["nmFilial"] == filial]
    return {
        "data": str(ontem),
        "filial": filial,
        "total_vendas": float(vendas_ontem["vlVenda"].sum())
    }


@app.get("/acumulado-mes")
def acumulado_mes(
    ano: int,
    mes: int,
    filial: Optional[str] = Query(default=None, description="Nome da filial")
):
    df = preparar_dados(carregar_dados())
    vendas_mes = df[(df["dtVenda"].dt.year == ano) & (df["dtVenda"].dt.month == mes)]
    if filial:
        vendas_mes = vendas_mes[vendas_mes["nmFilial"] == filial]
    return {"ano": ano, "mes": mes, "filial": filial, "acumulado": float(vendas_mes["vlVenda"].sum())}

@app.get("/previsao-por-filial")
def previsao_por_filial(
    ano: int,
    mes: int,
    filial: Optional[str] = Query(default=None, description="Nome da filial")
):
    df = preparar_dados(carregar_dados())
    vendas_mes = df[(df["dtVenda"].dt.year == ano) & (df["dtVenda"].dt.month == mes)]
    if filial:
        vendas_mes = vendas_mes[vendas_mes["nmFilial"] == filial]
        previsao = vendas_mes.groupby("nmFilial")["vlVenda"].sum().reset_index()
    else:
        previsao = vendas_mes.groupby("nmFilial")["vlVenda"].sum().reset_index()
    return previsao.to_dict(orient="records")

@app.get("/filial-mais-vendeu")
def filial_top_vendas(ano: int, mes: int, dia: int):
    df = preparar_dados(carregar_dados())
    vendas_mes = df[(df["dtVenda"].dt.year == ano) & (df["dtVenda"].dt.month == mes) & (df["dtVenda"].dt.day == dia)]
    top = vendas_mes.groupby("nmFilial")["vlVenda"].sum().idxmax()
    valor = vendas_mes.groupby("nmFilial")["vlVenda"].sum().max()
    return {"filial": top, "valor_total": float(valor)}

