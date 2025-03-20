import os

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime

# Definição da DAG
with DAG(
    dag_id="executa_notebooks_jupyter",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 3, 18),
    catchup=False
) as dag:
    base_dir = os.path.abspath(os.path.join(os.getcwd(), "../"))
    data_path_silver = os.path.join(base_dir, "lakehouse_srag/scripts/srag_silver.ipynb")
    data_path_gold = os.path.join(base_dir, "lakehouse_srag/scripts/srag_gold.ipynb")

    # Executa o notebook Silver
    task_silver = PapermillOperator(
        task_id="executa_silver",
        input_nb=data_path_silver,
        output_nb=data_path_silver
    )

    # Executa o notebook Gold após o Silver
    task_gold = PapermillOperator(
        task_id="executa_gold",
        input_nb=data_path_gold,
        output_nb=data_path_gold
    )

    # Define a ordem de execução
    task_silver >> task_gold
