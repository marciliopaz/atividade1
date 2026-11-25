"""
DAG ShopBrasil, um marketplace de e-commerce

Função: 
Coletar os produtos da API de catálogo (representada aqui pela 
FakeStore API), calcular as métricas por categoria e gravar o resultado na base analítica 
em PostgreSQL. 

- 1 função de busca
- 1 função de validação
- processamento paralelo por categoria usando Dynamic Task Mapping
- TaskGroups organizam extração, validação, transformação e carga
"""

from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
import pendulum
import logging

log = logging.getLogger(__name__)

def on_success_callback(context):
    task_id = context['task'].task_id
    log.info("Task: %s completada com sucesso", task_id)

def on_failure_callback(context):
    task_id = context['task'].task_id
    log.error("Falha na task: %s", task_id)

def on_retry_callback(context):
    task_id = context['task'].task_id
    exception = context['exception']
    log.warning("Nova tentativa da task: %s. Erro %s", task_id, exception)


@dag(
    dag_id='pipeline_shopbrasil',
    description="Pipeline com metricas de precos e separados por categoria",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    schedule="0 6 * * *", # Rodar todos os dias as 6hrs horário de São Paulo
    catchup=False,
    tags=['shopbrasil', 'preços' ,'atividade'],
)

def pipeline_shopbrasil():

    @task(retries=3, retry_delay=timedelta(seconds=10),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=5),
    on_failure_callback=on_failure_callback,
    on_retry_callback=on_retry_callback,
    on_success_callback=on_success_callback,
    )

    def buscar_produtos():
        """Busca os produtos da API de catálogo."""
        import requests

        url = "https://fakestoreapi.com/products"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            dados = response.json()

            if not isinstance(dados, list):
                raise ValueError("Resposta da API não está no formato esperado (lista).")
            if not dados:
                raise ValueError("Lista de produtos da API não retornou produtos e/ou vazia")

            log.info("Lista de produtos extraídos com sucesso. Total: %s", len(dados))
            return dados
        
        except requests.exceptions.HTTPError:
            log.exception("Erro HTTP ao consultar a API FakeStore. Status: %s",response.status_code)
            raise
        except requests.exceptions.Timeout:
            log.exception("Timeout ao consultar a API FakeStore")
            raise
        except requests.exceptions.RequestException:
            log.exception("Falha ao buscar produtos da API FakeStore")
            raise
        except Exception:
            log.exception("Erro durante a coleta de produtos")
            raise

    @task
    def validar(dados):
        """Valida os campos obrigatórios de cada produto."""
        if not dados:
            raise ValueError("Lista de produtos vazia")

        campos = ('id', 'title', 'price', 'category')
        produtos_validos = []

        for produto in dados:
            if not isinstance(produto, dict):
                raise ValueError("Produto inválido: item não é um objeto")

            faltantes = [campo for campo in campos if campo not in produto]
            if faltantes:
                raise ValueError(
                    f"Dados incompletos: faltam os campos {', '.join(faltantes)}"
                )

            if produto['price'] is None:
                raise ValueError(f"Preço inválido para o produto {produto['id']}")

            try:
                preco = float(produto['price'])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Preço inválido para o produto {produto['id']}") from exc

            if preco < 0:
                raise ValueError(f"Preço negativo para o produto {produto['id']}")

            produto['price'] = preco
            produtos_validos.append(produto)

        return produtos_validos  # repassa os dados validados para a transformação

    @task
    def preparar_categorias(dados):
        """Separa os produtos por categoria para processamento paralelo."""
        categorias = sorted(set(produto["category"] for produto in dados))

        return [
            {
                "categoria": categoria,
                "produtos": [
                    produto for produto in dados
                    if produto["category"] == categoria
                ]
            }
            for categoria in categorias
            if categoria is not None
        ]

    @task(pool="ecommerce_pool")
    def transformar_categoria(item):
        """Calcula as métricas de preço de uma categoria."""
        import pandas as pd

        categoria = item["categoria"]
        df = pd.DataFrame(item["produtos"])

        return {
            "categoria": categoria,
            "quantidade": int(df["id"].count()),
            "preco_medio": round(float(df["price"].mean()), 2),
            "preco_minimo": round(float(df["price"].min()), 2),
            "preco_maximo": round(float(df["price"].max()), 2),
        }

    @task
    def consolidar(resultados):
        """Junta os resultados de todas as categorias"""
        return resultados

    @task
    def salvar_no_banco(registros):
        """Grava as métricas por categoria no PostgreSQL."""

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.operators.python import get_current_context

        if not registros:
            raise ValueError("Nenhum registro para salvar")

        context = get_current_context()

        data_consulta = (
            context["logical_date"]
            .in_timezone("America/Sao_Paulo")
            .date()
        )

        dag_run_id = context["dag_run"].run_id

        hook = PostgresHook(postgres_conn_id="postgres_lab")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pricing_metrics (
                    id SERIAL PRIMARY KEY,
                    data_consulta DATE NOT NULL,
                    categoria VARCHAR(100) NOT NULL,
                    quantidade_produtos INTEGER NOT NULL,
                    preco_medio NUMERIC(12,2) NOT NULL,
                    preco_minimo NUMERIC(12,2) NOT NULL,
                    preco_maximo NUMERIC(12,2) NOT NULL,
                    dag_run_id VARCHAR(250),
                    inserido_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                    CONSTRAINT uk_pricing_categoria_data
                        UNIQUE (data_consulta, categoria)
                )
            """)

            sql = """
                INSERT INTO pricing_metrics (
                    data_consulta,
                    categoria,
                    quantidade_produtos,
                    preco_medio,
                    preco_minimo,
                    preco_maximo,
                    dag_run_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)

                ON CONFLICT (data_consulta, categoria)
                DO UPDATE SET
                    quantidade_produtos = EXCLUDED.quantidade_produtos,
                    preco_medio = EXCLUDED.preco_medio,
                    preco_minimo = EXCLUDED.preco_minimo,
                    preco_maximo = EXCLUDED.preco_maximo,
                    dag_run_id = EXCLUDED.dag_run_id
            """

            for reg in registros:
                cursor.execute(
                    sql,
                    (
                        data_consulta,
                        reg["categoria"],
                        reg["quantidade"],
                        reg["preco_medio"],
                        reg["preco_minimo"],
                        reg["preco_maximo"],
                        dag_run_id,
                    )
                )

            conn.commit()

            log.info(
                "Salvas %s categorias para %s",
                len(registros),
                data_consulta,
            )

            return len(registros)

        except Exception:
            conn.rollback()
            log.exception("Erro ao salvar métricas no PostgreSQL")
            raise

        finally:
            cursor.close()
            conn.close()


        # ── Execução do pipeline ────────────────────────────────────────────────
        # Organização do pipeline:
        # Extração -> Validação -> Transformação -> Carga
        #
        # O Dynamic Task Mapping cria uma execução da tarefa
        # transformar_categoria para cada categoria encontrada na API.
    with TaskGroup('extracao'):
        produtos = buscar_produtos()

    with TaskGroup('validacao'):
        produtos_validados = validar(produtos)

    with TaskGroup('transformacao'):
        categorias = preparar_categorias(produtos_validados)
        metricas_categoria = transformar_categoria.expand(item=categorias)
        metricas_final = consolidar(metricas_categoria)

    with TaskGroup("carga"):
        salvar_no_banco(metricas_final)


# Instanciar o DAG
dag_instance = pipeline_shopbrasil()
