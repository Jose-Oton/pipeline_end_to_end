#1. Documentación de un DAG
"""
## PYSPARK DAG
Este pipeline ingesta datos de postgres, copia archvios a Cloud Storage, ejecuta un job con PySpark dentro de Dataproc,
genera una dataset y crea vistas para su consulta en BigQuery.
"""

from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from extract_data import data_ingestion,copy_files
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator,BigQueryCreateEmptyTableOperator
from airflow.utils import trigger_rule


#2. Utilizar Variables
PROJECT_ID = Variable.get("project")
STORAGE_BUCKET = Variable.get("storage_bucket")
DATASET_NAME = "analytics_dwh"
default_dag_args = {
    "start_date": days_ago(1),
    "owner": "José Otón"
}


# DEFINIMOS DAG
with DAG(
    dag_id='1st_End_to_end',
    description='Copy files,extract data and running a PySpark Job on GCP',
    schedule_interval='@daily',
    default_args=default_dag_args,
    max_active_runs=1,
    user_defined_macros={"project": PROJECT_ID},
) as dag:

    dag.doc_md = __doc__ #Para documentar un DAG

    ingest_data = PythonOperator(
        task_id='Ingest_data_from_postgres',
        python_callable=data_ingestion,
        dag=dag,
    )
    ingest_data.doc_md = """## Ingesta datos de postgresql
    """

    create_dataproc = DataprocCreateClusterOperator(
        task_id="create_dataproc",
        project_id='{{ project }}',
        cluster_name="spark-cluster-{{ ds_nodash }}",
        num_workers=2,
        storage_bucket=STORAGE_BUCKET,
        region="us-central1"
        #metadata={'PIP_PACKAGES':'datetime praw google-cloud-storage'}
            )

    create_dataproc.doc_md = """## Crear cluster de Dataproc
    Crea un cluster de Dataproc en el proyecto de GCP
    """

    pyspark_job = DataprocSubmitJobOperator(
        task_id="pyspark_job",
        project_id=PROJECT_ID,
        location='us-central1',
           job={
            'reference': {'project_id': '{{ project }}',
            'job_id': '{{task.task_id}}_{{ds_nodash}}_2446afcc_a'}, ## si puede haber cambio.
            'placement': {'cluster_name': 'spark-cluster-{{ ds_nodash }}'},
            'labels': {'airflow-version': 'v2-1-0'},
            'pyspark_job': {
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar',
                'gs://hadoop-lib/gcs/gcs-connector-hadoop2-2.1.1.jar'],
                'main_python_file_uri': 'gs://your_bucket/pysparkJobs/pyspark_transformation.py'
            }
        },
        gcp_conn_id='google_cloud_default'          
    )

    pyspark_job.doc_md = """## Spark Transformation
    Ejecuta las transformaciones con Spark.
    """

    copyCsv =  PythonOperator(
        task_id='Copy_files',
        python_callable=copy_files,
        dag=dag
        )
    copyCsv.doc_md = """## Copia un archivos al storage antiguo
    """

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name="spark-cluster-{{ ds_nodash }}",
        trigger_rule="all_done",
        region='us-central1'
        #zone='us-central1-a'
    )
    delete_cluster.doc_md = """## Borrar Cluster de Dataproc
    Elimina el cluster de Dataproc.
    """

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", dataset_id=DATASET_NAME)
    create_dataset.doc_md = """## Crea un dataset en BigQuery
    """

    create_customers_view = BigQueryCreateEmptyTableOperator(
        task_id="create_customers_view",
        dataset_id=DATASET_NAME,
        table_id="top_customers_view",
        view={
            "query": f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.top_customers`",
            "useLegacySql": False,
        },
    )
    create_customers_view.doc_md = """## Crea una vista en BQ de la tabla top_customers
    """

    create_top_products_materialized_view = BigQueryCreateEmptyTableOperator(
        task_id="create_top_products_materialized_view",
        dataset_id=DATASET_NAME,
        table_id="top_products_materialized_view",
        materialized_view={
            "query": f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.top_products`",
            "enableRefresh": True,
            "refreshIntervalMs": 2000000,
        },
    )
    create_top_products_materialized_view.doc_md = """## Crea una vista Materializada
    en BQ de la tabla top_products
    """
    create_customers_free_debt_materialized_view = BigQueryCreateEmptyTableOperator(
        task_id="create_customers_free_debt_materialized_view",
        dataset_id=DATASET_NAME,
        table_id="customers_free_debt_materialized_view",
        materialized_view={
            "query": f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.customers_free_debt`",
            "enableRefresh": True,
            "refreshIntervalMs": 2000000,
        },
    )
    create_customers_free_debt_materialized_view.doc_md = """## Crea una vista Materializada
    en BQ de la tabla customers_free_debt
    """

# SETEAR LAS DEPEDENDENCIAS DEL DAG
    (ingest_data >> copyCsv >> create_dataset >> create_dataproc >>  pyspark_job
     >> delete_cluster >> create_customers_view
     >> create_top_products_materialized_view >> create_customers_free_debt_materialized_view)
