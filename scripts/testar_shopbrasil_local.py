#!/usr/bin/env python3
"""
Testa localmente as etapas de busca, validacao e transformacao do DAG ShopBrasil,
sem precisar do Airflow ou do Docker.

Uso:
  python scripts/testar_shopbrasil_local.py
  python scripts/testar_shopbrasil_local.py --step fetch
  python scripts/testar_shopbrasil_local.py --step validate
  python scripts/testar_shopbrasil_local.py --step transform

Dependencias:
  pip install requests pandas
"""

import argparse
import json
import logging
import sys
import tempfile
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(tempfile.gettempdir()) / "shopbrasil_local"
RAW_DATA_FILE = DATA_DIR / "produtos_brutos.json"
VALIDATED_DATA_FILE = DATA_DIR / "produtos_validados.json"
METRICS_FILE = DATA_DIR / "metricas_categoria.json"


def buscar_produtos() -> list[dict]:
    """Replica a task buscarProdutos do DAG."""
    import requests

    url = "https://fakestoreapi.com/products"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    produtos = response.json()
    log.info("%d produtos recebidos da API", len(produtos))
    return produtos


def validar(dados: list[dict]) -> list[dict]:
    """Replica a task validar do DAG."""
    if not dados:
        raise ValueError("Lista de produtos vazia")

    campos = ("id", "title", "price", "category")
    produtos_validos = []

    for produto in dados:
        if not isinstance(produto, dict):
            raise ValueError("Produto invalido: item nao e um objeto")

        faltantes = [campo for campo in campos if campo not in produto]
        if faltantes:
            raise ValueError(
                f"Dados incompletos: faltam os campos {', '.join(faltantes)}"
            )

        if produto["price"] is None:
            raise ValueError(f"Preco invalido para o produto {produto['id']}")

        try:
            preco = float(produto["price"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Preco invalido para o produto {produto['id']}"
            ) from exc

        if preco < 0:
            raise ValueError(f"Preco negativo para o produto {produto['id']}")

        produto["price"] = preco
        produtos_validos.append(produto)

    return produtos_validos


def transformar(dados: list[dict]) -> list[dict]:
    """Replica a task transformar do DAG."""
    import pandas as pd

    df = pd.DataFrame(dados)
    metricas = (
        df.groupby("category", dropna=False)
        .agg(
            quantidade=("id", "count"),
            preco_medio=("price", "mean"),
            preco_total=("price", "sum"),
            preco_maximo=("price", "max"),
            preco_minimo=("price", "min"),
        )
        .reset_index()
        .sort_values("category")
    )

    metricas["preco_medio"] = metricas["preco_medio"].round(2)
    metricas["preco_total"] = metricas["preco_total"].round(2)
    metricas["preco_maximo"] = metricas["preco_maximo"].round(2)
    metricas["preco_minimo"] = metricas["preco_minimo"].round(2)
    metricas["quantidade"] = metricas["quantidade"].astype(int)
    return metricas.to_dict(orient="records")


def salvar_json(caminho: Path, dados: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, indent=2, ensure_ascii=False)
    log.info("Dados salvos em %s", caminho)


def carregar_json(caminho: Path) -> list[dict]:
    if not caminho.exists():
        raise FileNotFoundError(
            f"Arquivo nao encontrado: {caminho}. "
            "Execute primeiro a etapa anterior ou use --step all."
        )
    with caminho.open(encoding="utf-8") as arquivo:
        return json.load(arquivo)


def mostrar_preview(metricas: list[dict]) -> None:
    """Exibe as metricas calculadas por categoria."""
    import pandas as pd

    df = pd.DataFrame(metricas)
    print("\nMetricas por categoria:")
    print(
        df.rename(
            columns={
                "category": "Categoria",
                "quantidade": "Quantidade",
                "preco_medio": "Preco medio",
                "preco_total": "Preco total",
                "preco_maximo": "Preco maximo",
                "preco_minimo": "Preco minimo",
            }
        ).to_string(index=False)
    )
    print(f"\nTotal de categorias: {len(df)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Testa localmente o pipeline ShopBrasil sem Airflow"
    )
    parser.add_argument(
        "--step",
        choices=["fetch", "validate", "transform", "all"],
        default="all",
        help="Etapa a executar (padrao: all)",
    )
    args = parser.parse_args()

    try:
        if args.step in ("fetch", "all"):
            log.info("=== STEP 1: buscar produtos ===")
            produtos = buscar_produtos()
            salvar_json(RAW_DATA_FILE, produtos)
        elif args.step == "validate":
            produtos = carregar_json(RAW_DATA_FILE)
        else:
            produtos = carregar_json(VALIDATED_DATA_FILE)

        if args.step in ("validate", "all"):
            log.info("=== STEP 2: validar produtos ===")
            produtos = validar(produtos)
            salvar_json(VALIDATED_DATA_FILE, produtos)

        if args.step in ("transform", "all"):
            log.info("=== STEP 3: calcular metricas ===")
            metricas = transformar(produtos)
            salvar_json(METRICS_FILE, metricas)
            mostrar_preview(metricas)

        log.info("Teste concluido. Para gravar no PostgreSQL, execute o DAG no Airflow.")
    except ImportError as exc:
        log.error("Dependencia faltando: %s", exc)
        log.error("Instale com: pip install requests pandas")
        sys.exit(1)
    except Exception:
        log.exception("Erro durante o teste local")
        raise


if __name__ == "__main__":
    main()
