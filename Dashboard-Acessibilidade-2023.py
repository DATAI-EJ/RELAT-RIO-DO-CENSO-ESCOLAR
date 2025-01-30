from dash import Dash, html, dcc, Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_USER = os.getenv('DB_USER', '2312120036_Joel')
DB_PASSWORD = os.getenv('DB_PASSWORD', '2312120036_Joel')
DB_HOST = os.getenv('DB_HOST', 'dataiesb.iesbtech.com.br')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', '2312120036_Joel')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?options=-csearch_path=CENSO_FILTRADO_EDUCAÇÃO"
engine = create_engine(DATABASE_URL)

external_stylesheets = [dbc.themes.BOOTSTRAP]
app = Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Acessibilidade Escolar 2023"

def executar_query(query):
    """Executa uma query SQL e retorna um DataFrame."""
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logging.info(f"Dados retornados: {df.head()}")
            return df
    except Exception as e:
        logging.error(f"Erro ao executar query: {e}")
        return pd.DataFrame()

def carregar_dados():
    """Carrega e processa os dados do banco de dados."""
    query = """
    SELECT e."SG_UF" AS "Estado", 
           e."NO_REGIAO" AS "Região", 
           e."TP_DEPENDENCIA" AS "Dependência",
           e."IN_INTERNET" AS "Internet", 
           e."IN_ACESSIBILIDADE_CORRIMAO" AS "Corrimão",
           e."IN_ACESSIBILIDADE_ELEVADOR" AS "Elevador", 
           e."IN_ACESSIBILIDADE_PISOS_TATEIS" AS "Pisos Táteis",
           e."IN_ACESSIBILIDADE_VAO_LIVRE" AS "Vão Livre", 
           e."IN_ACESSIBILIDADE_RAMPAS" AS "Rampas",
           e."IN_ACESSIBILIDADE_SINAL_SONORO" AS "Sinal Sonoro", 
           e."IN_ACESSIBILIDADE_SINAL_TATIL" AS "Sinal Tátil",
           e."IN_ACESSIBILIDADE_SINAL_VISUAL" AS "Sinal Visual", 
           e."TP_SITUACAO_FUNCIONAMENTO"
    FROM "CENSO_FILTRADO_EDUCAÇÃO"."arquivo_filtrado" e
    WHERE e."TP_SITUACAO_FUNCIONAMENTO" = 1;
    """
    df = executar_query(query)

    if df.empty:
        logging.warning("Nenhum dado foi retornado da consulta SQL.")
        return pd.DataFrame(), []

    indicadores = ['Corrimão', 'Elevador', 'Pisos Táteis', 'Vão Livre', 'Rampas',
                   'Sinal Sonoro', 'Sinal Tátil', 'Sinal Visual']
    try:
        df['Tipo Escola'] = df['Dependência'].apply(lambda x: 'Privada' if x == 4 else 'Pública')
        df[indicadores] = df[indicadores].fillna(0).astype(int)
        df['Todas Acessibilidades'] = df[indicadores].sum(axis=1) == len(indicadores)
        df['Nenhuma Acessibilidade'] = df[indicadores].sum(axis=1) == 0
        logging.info(f"Dados processados: {df.head()}")
    except KeyError as e:
        logging.error(f"Erro ao processar colunas: {e}")
        return pd.DataFrame(), []

    return df, indicadores

df, INDICADORES = carregar_dados()

if df.empty:
    logging.error("O DataFrame está vazio após carregar os dados.")
else:
    logging.info(f"Colunas disponíveis: {df.columns.tolist()}")

def gerar_grafico_pizza(df):
    if df.empty:
        return go.Figure()

    try:
        internet = df.groupby(['Internet']).size().reset_index(name='Quantidade')
        internet['Internet'] = internet['Internet'].replace({0: 'Não', 1: 'Sim'})

        fig = px.pie(
            internet, names='Internet', values='Quantidade',
            title='Distribuição de Acesso à Internet',
            color='Internet', color_discrete_map={'Não': '#d62728', 'Sim': '#2ca02c'},
            hole=0.4
        )
        fig.update_traces(textinfo='percent+label', pull=[0.1, 0])
        fig.update_layout(showlegend=False, margin=dict(t=30, b=0, l=0, r=0))
        return fig
    except Exception as e:
        logging.error(f"Erro ao gerar gráfico de pizza: {e}")
        return go.Figure()

def gerar_grafico_barras(df):
    if df.empty:
        return go.Figure()

    try:
        contagem = df[INDICADORES].apply(pd.Series.value_counts).fillna(0).T
        contagem = contagem.div(contagem.sum(axis=1), axis=0) * 100
        contagem.columns = contagem.columns.astype(str)

        fig = go.Figure()
        for key, label in {'0': 'Não', '1': 'Sim'}.items():
            fig.add_bar(
                name=label,
                x=contagem.index,
                y=contagem.get(key, pd.Series([0] * len(contagem))),
                text=contagem.get(key, pd.Series([0] * len(contagem))).round(1).astype(str) + '%',
                textposition='outside',
                marker_color={'Não': '#d62728', 'Sim': '#2ca02c'}[label]
            )

        fig.update_layout(
            barmode='stack',
            title='Acessibilidade das Escolas',
            margin=dict(t=30, b=0, l=0, r=0),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        return fig
    except Exception as e:
        logging.error(f"Erro ao gerar gráfico de barras: {e}")
        return go.Figure()

def gerar_treemap(df):
    if df.empty:
        return go.Figure()

    try:
        treemap_data = df[INDICADORES].sum().reset_index()
        treemap_data.columns = ['Indicador', 'Quantidade']

        fig = px.treemap(
            treemap_data,
            path=['Indicador'],
            values='Quantidade',
            title="Proporção de Indicadores de Acessibilidade",
            color='Quantidade',
            color_continuous_scale='Blues'
        )
        fig.update_layout(margin=dict(t=30, b=0, l=0, r=0))
        return fig
    except Exception as e:
        logging.error(f"Erro ao gerar treemap: {e}")
        return go.Figure()

    
@app.callback(
    [Output("total-acessibilidade", "children"),
     Output("sem-acessibilidade", "children"),
     Output("grafico-pizza", "figure"),
     Output("grafico-barras", "figure"),
     Output("grafico-radar", "figure"),
     Output("total-escolas", "children")],
    [Input("filtro-estado", "value"),
     Input("filtro-regiao", "value"),
     Input("filtro-dependencia", "value")]
)
def atualizar_dashboard(estado, regiao, dependencia):
    df_filtrado = df
    if estado:
        df_filtrado = df_filtrado[df_filtrado['Estado'] == estado]
    if regiao:
        df_filtrado = df_filtrado[df_filtrado['Região'] == regiao]
    if dependencia:
        df_filtrado = df_filtrado[df_filtrado['Tipo Escola'] == dependencia]

    logging.info(f"Filtros aplicados - Estado: {estado}, Região: {regiao}, Dependência: {dependencia}")
    logging.info(f"Dados filtrados: {df_filtrado.head()}")

    total_acessibilidade = len(df_filtrado[df_filtrado['Todas Acessibilidades']])
    sem_acessibilidade = len(df_filtrado[df_filtrado['Nenhuma Acessibilidade']])
    total_escolas = len(df_filtrado)

    fig_pizza = gerar_grafico_pizza(df_filtrado)
    fig_barras = gerar_grafico_barras(df_filtrado)
    fig_radar = gerar_treemap(df_filtrado)

    return (f"{total_acessibilidade:,}", f"{sem_acessibilidade:,}",
            fig_pizza, fig_barras, fig_radar, f"{total_escolas:,}")

app.layout = dbc.Container([
    html.H1("Acessibilidade Escolar 2023", className="text-center text-dark mt-4 mb-2"),
    html.Hr(),
    html.P("Este dashboard apresenta dados sobre a acessibilidade das escolas brasileiras, "
           "incluindo a presença de internet e características de acessibilidades físicas.",
           className="text-center text-muted mb-4"),
    html.Hr(),
    dbc.Row([
        dbc.Col([
            html.Label("Estado", className="form-label fw-bold"),
            dcc.Dropdown(id='filtro-estado', options=[{'label': e, 'value': e} for e in df['Estado'].dropna().unique()], placeholder="Selecione um Estado", className="mb-2 border-primary shadow")
        ], width=4),
        dbc.Col([
            html.Label("Região", className="form-label fw-bold"),
            dcc.Dropdown(id='filtro-regiao', options=[{'label': r, 'value': r} for r in df['Região'].dropna().unique()], placeholder="Selecione uma Região", className="mb-2 border-primary shadow")
        ], width=4),
        dbc.Col([
            html.Label("Dependência", className="form-label fw-bold"),
            dcc.Dropdown(id='filtro-dependencia', options=[{'label': t, 'value': t} for t in df['Tipo Escola'].dropna().unique()], placeholder="Selecione uma Dependência", className="mb-2 border-primary shadow")
        ], width=4),
    ]),
    html.Hr(),
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("Total de Escolas", className="card-title"),
                html.H2(id="total-escolas", className="card-text text-primary")
            ], className="text-center")
        ], color="light", inverse=False, style={"border-radius": "15px"})),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("Escolas com Todas as Acessibilidades", className="card-title"),
                html.H2(id="total-acessibilidade", className="card-text text-success")
            ], className="text-center")
        ], color="light", inverse=False, style={"border-radius": "15px"})),
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("Escolas sem Acessibilidade", className="card-title"),
                html.H2(id="sem-acessibilidade", className="card-text text-danger")
            ], className="text-center")
        ], color="light", inverse=False, style={"border-radius": "15px"})),
    ], className="mb-4"),
    html.Hr(),
    dbc.Row([
        dbc.Col(dcc.Graph(id='grafico-pizza', config={'displayModeBar': False}), width=6),
        dbc.Col(dcc.Graph(id='grafico-barras', config={'displayModeBar': False}), width=6),
    ]),
    html.Hr(),
    dbc.Row([
        dbc.Col(dcc.Graph(id='grafico-radar', config={'displayModeBar': False}), width=12),
    ])
], fluid=True, style={"padding": "20px"})

if __name__ == '__main__':
    app.run(debug=True)
