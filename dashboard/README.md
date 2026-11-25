# Dashboard ShopBrasil

O dashboard em Streamlit acompanha as execuções da DAG `pipeline_shopbrasil`
pela API REST do Airflow e consulta as métricas gravadas em `pricing_metrics`
no PostgreSQL.

Com os serviços do Airflow e PostgreSQL configurados pelo projeto, inicie o
dashboard na raiz do projeto:

```powershell
docker compose up -d --build shopbrasil-dashboard
```

Acesse <http://localhost:8501>. O painel atualiza os dados a cada 30 segundos;
use **Atualizar agora** para forçar uma atualização. Por padrão, as credenciais
e endereços são os do ambiente de laboratório definido no `docker-compose.yml`.
Para outro ambiente, configure `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`,
`DB_PASSWORD`, `AIRFLOW_API_URL`, `AIRFLOW_UI_URL`, `AIRFLOW_USER` e
`AIRFLOW_PASSWORD`.

O histórico de preços é diário por categoria. A DAG atualiza a combinação
data/categoria se for executada novamente no mesmo dia.
