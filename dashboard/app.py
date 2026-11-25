import os
from datetime import date, timedelta
from urllib.parse import quote

import pandas as pd
import psycopg2
import requests
import streamlit as st


DAG_ID = "pipeline_shopbrasil"
AIRFLOW_API_URL = os.getenv(
    "AIRFLOW_API_URL", "http://localhost:8080/api/v1"
).rstrip("/")
AIRFLOW_UI_URL = os.getenv("AIRFLOW_UI_URL", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

METRIC_COLUMNS = [
    "data_consulta",
    "categoria",
    "quantidade_produtos",
    "preco_medio",
    "preco_minimo",
    "preco_maximo",
    "dag_run_id",
]

STATE_LABELS = {
    "queued": "Na fila",
    "scheduled": "Agendada",
    "running": "Em execução",
    "success": "Sucesso",
    "failed": "Falhou",
    "up_for_retry": "Aguardando nova tentativa",
    "up_for_reschedule": "Aguardando reagendamento",
    "skipped": "Ignorada",
    "removed": "Removida",
    "deferred": "Adiada",
}

st.set_page_config(page_title="ShopBrasil | Operação", page_icon="🛒", layout="wide")
st.title("ShopBrasil | Monitoramento do e-commerce")
st.caption(
    "Acompanhamento diário do pipeline de preços e métricas por categoria. "
    "Os valores da FakeStore API são apresentados em US$."
)

if st.button("Atualizar agora"):
    st.cache_data.clear()
    st.rerun()


def api_get(path, params=None):
    response = requests.get(
        f"{AIRFLOW_API_URL}/{path.lstrip('/')}",
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        params=params,
        timeout=(3, 10),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("A API do Airflow retornou uma resposta inválida.")
    return payload


@st.cache_data(ttl=30)
def carregar_execucoes():
    dag_id = quote(DAG_ID, safe="")
    payload = api_get(
        f"dags/{dag_id}/dagRuns",
        params={"limit": 10, "order_by": "-execution_date"},
    )
    runs = payload.get("dag_runs", [])
    if not isinstance(runs, list):
        raise ValueError("A API do Airflow retornou uma lista de execuções inválida.")
    return runs


@st.cache_data(ttl=30)
def carregar_tarefas(dag_run_id):
    dag_id = quote(DAG_ID, safe="")
    run_id = quote(dag_run_id, safe="")
    payload = api_get(f"dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    tasks = payload.get("task_instances", [])
    if not isinstance(tasks, list):
        raise ValueError("A API do Airflow retornou uma lista de tarefas inválida.")
    return tasks


@st.cache_data(ttl=30)
def carregar_metricas():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5433")),
        dbname=os.getenv("DB_NAME", "labdb"),
        user=os.getenv("DB_USER", "lab"),
        password=os.getenv("DB_PASSWORD", "lab123"),
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    data_consulta,
                    categoria,
                    quantidade_produtos,
                    preco_medio,
                    preco_minimo,
                    preco_maximo,
                    dag_run_id
                FROM pricing_metrics
                ORDER BY data_consulta, categoria
                """
            )
            rows = cursor.fetchall()
    finally:
        conn.close()

    df = pd.DataFrame(rows, columns=METRIC_COLUMNS)
    if not df.empty:
        df["data_consulta"] = pd.to_datetime(df["data_consulta"]).dt.date
        for column in ("preco_medio", "preco_minimo", "preco_maximo"):
            df[column] = pd.to_numeric(df[column], errors="raise")
        df["quantidade_produtos"] = pd.to_numeric(
            df["quantidade_produtos"], errors="raise"
        ).astype(int)
    return df


def rotulo_estado(state):
    if not state:
        return "Pendente"
    return STATE_LABELS.get(str(state).lower(), str(state))


def formatar_data_hora(value):
    if not value:
        return "—"
    timestamp = pd.to_datetime(value, utc=True)
    return timestamp.tz_convert("America/Sao_Paulo").strftime("%d/%m/%Y %H:%M:%S")


def formatar_duracao(run):
    duration = run.get("duration")
    if duration is None and run.get("start_date") and run.get("end_date"):
        start = pd.to_datetime(run["start_date"], utc=True)
        end = pd.to_datetime(run["end_date"], utc=True)
        duration = (end - start).total_seconds()
    if duration is None:
        return "Em andamento" if run.get("start_date") else "—"

    total_seconds = max(0, int(duration))
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02}min"
    if minutes:
        return f"{minutes}min {seconds:02}s"
    return f"{seconds}s"


def rotulo_execucao(run):
    logical_date = run.get("logical_date") or run.get("execution_date")
    run_id = run.get("dag_run_id", "execução sem identificador")
    return (
        f"{formatar_data_hora(logical_date)} · "
        f"{rotulo_estado(run.get('state'))} · {run_id}"
    )


st.subheader("Execução da DAG")
st.link_button("Abrir interface do Airflow", AIRFLOW_UI_URL)

try:
    execucoes = carregar_execucoes()
except requests.exceptions.RequestException as exc:
    st.error(f"Não foi possível consultar a API do Airflow: {exc}")
    execucoes = []
except ValueError as exc:
    st.error(f"Resposta inválida da API do Airflow: {exc}")
    execucoes = []

if not execucoes:
    st.info("Nenhuma execução encontrada para a DAG pipeline_shopbrasil.")
else:
    execucao_selecionada = st.selectbox(
        "Execução para acompanhamento",
        execucoes,
        index=0,
        format_func=rotulo_execucao,
    )
    logical_date = (
        execucao_selecionada.get("logical_date")
        or execucao_selecionada.get("execution_date")
    )
    dag_cols = st.columns(3)
    dag_cols[0].metric(
        "Status da DAG", rotulo_estado(execucao_selecionada.get("state"))
    )
    dag_cols[1].metric("Data lógica", formatar_data_hora(logical_date))
    dag_cols[2].metric("Duração", formatar_duracao(execucao_selecionada))

    run_id = execucao_selecionada.get("dag_run_id")
    if run_id:
        try:
            tarefas = carregar_tarefas(run_id)
        except requests.exceptions.RequestException as exc:
            st.error(f"Não foi possível consultar as tarefas desta execução: {exc}")
            tarefas = []
        except ValueError as exc:
            st.error(f"Resposta inválida das tarefas no Airflow: {exc}")
            tarefas = []

        if tarefas:
            linhas_tarefas = []
            for tarefa in tarefas:
                task_name = tarefa.get("task_id", "Tarefa")
                map_index = tarefa.get("map_index", -1)
                if map_index is not None and map_index >= 0:
                    task_name = f"{task_name} (categoria {map_index + 1})"
                linhas_tarefas.append(
                    {
                        "Tarefa": task_name,
                        "Status": rotulo_estado(tarefa.get("state")),
                        "Tentativa": tarefa.get("try_number", "—"),
                        "Máximo de tentativas": tarefa.get("max_tries", "—"),
                        "Início": formatar_data_hora(tarefa.get("start_date")),
                        "Fim": formatar_data_hora(tarefa.get("end_date")),
                    }
                )
            st.dataframe(
                pd.DataFrame(linhas_tarefas),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Ainda não há tarefas registradas para esta execução.")

    with st.expander("Últimas execuções"):
        historico = [
            {
                "Data lógica": formatar_data_hora(
                    run.get("logical_date") or run.get("execution_date")
                ),
                "Status": rotulo_estado(run.get("state")),
                "Duração": formatar_duracao(run),
                "ID": run.get("dag_run_id", "—"),
            }
            for run in execucoes
        ]
        st.dataframe(pd.DataFrame(historico), use_container_width=True, hide_index=True)

st.divider()
st.subheader("Indicadores processados")

try:
    metricas = carregar_metricas()
except psycopg2.errors.UndefinedTable:
    st.info(
        "A tabela de métricas ainda não existe. Ela será criada quando a task "
        "`carga.salvar_no_banco` concluir sua primeira execução."
    )
    metricas = pd.DataFrame(columns=METRIC_COLUMNS)
except psycopg2.Error as exc:
    st.error(f"Não foi possível consultar o PostgreSQL: {exc}")
    metricas = pd.DataFrame(columns=METRIC_COLUMNS)

if not metricas.empty:
    min_date = metricas["data_consulta"].min()
    max_date = metricas["data_consulta"].max()
    default_start = max(min_date, max_date - timedelta(days=29))

    with st.sidebar:
        st.header("Filtros")
        date_range = st.date_input(
            "Período",
            value=(default_start, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        categorias = sorted(metricas["categoria"].dropna().unique().tolist())
        categorias_selecionadas = st.multiselect(
            "Categorias",
            options=categorias,
            default=categorias,
        )

    if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
        inicio, fim = date_range
    elif isinstance(date_range, date):
        inicio = fim = date_range
    else:
        inicio, fim = default_start, max_date

    filtradas = metricas[
        metricas["data_consulta"].between(inicio, fim)
        & metricas["categoria"].isin(categorias_selecionadas)
    ]

    if filtradas.empty:
        st.info("Não há métricas para os filtros selecionados.")
    else:
        data_mais_recente = filtradas["data_consulta"].max()
        resumo = filtradas[filtradas["data_consulta"] == data_mais_recente]
        total_produtos = int(resumo["quantidade_produtos"].sum())
        preco_medio = (
            float(
                (
                    resumo["preco_medio"] * resumo["quantidade_produtos"]
                ).sum()
                / total_produtos
            )
            if total_produtos
            else 0
        )
        preco_minimo = float(resumo["preco_minimo"].min())
        preco_maximo = float(resumo["preco_maximo"].max())

        kpis = st.columns(4)
        kpis[0].metric("Produtos analisados", f"{total_produtos:,}".replace(",", "."))
        kpis[1].metric("Preço médio ponderado", f"US$ {preco_medio:,.2f}")
        kpis[2].metric("Menor preço", f"US$ {preco_minimo:,.2f}")
        kpis[3].metric("Maior preço", f"US$ {preco_maximo:,.2f}")
        st.caption(
            f"Indicadores do processamento mais recente no período: "
            f"{data_mais_recente.strftime('%d/%m/%Y')}."
        )

        chart_cols = st.columns(2)
        with chart_cols[0]:
            st.markdown("#### Preço médio por categoria")
            st.bar_chart(
                resumo.set_index("categoria")["preco_medio"],
                x_label="Categoria",
                y_label="Preço médio (US$)",
            )
        with chart_cols[1]:
            st.markdown("#### Evolução do preço médio")
            tendencia = filtradas.pivot_table(
                index="data_consulta",
                columns="categoria",
                values="preco_medio",
                aggfunc="mean",
            ).sort_index()
            st.line_chart(
                tendencia,
                x_label="Data",
                y_label="Preço médio (US$)",
            )

        st.markdown("#### Métricas por categoria — último processamento")
        tabela = resumo[
            [
                "categoria",
                "quantidade_produtos",
                "preco_medio",
                "preco_minimo",
                "preco_maximo",
            ]
        ].rename(
            columns={
                "categoria": "Categoria",
                "quantidade_produtos": "Produtos",
                "preco_medio": "Preço médio (US$)",
                "preco_minimo": "Preço mínimo (US$)",
                "preco_maximo": "Preço máximo (US$)",
            }
        )
        st.dataframe(tabela, use_container_width=True, hide_index=True)
else:
    st.info(
        "As métricas serão exibidas após a task `carga.salvar_no_banco` "
        "concluir a primeira execução da DAG."
    )
