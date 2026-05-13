"""
DAG: dynamic_partition_creation
Description:
- checks for rows in default partition,
- calls stored procedure to create missing partitions and move rows,
- verifies result and writes basic stats.
Schedule: every 6 hours.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_default"
PARTITION_TABLE = "public.trains_with_partition"
DEFAULT_PARTITION = "public.trains_default"


def on_task_failure(context):
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    logger.error("Task '%s' in DAG '%s' failed at %s", ti.task_id, dag_id, context["ts"])


def check_for_rows_in_default(**context):
    """Branch: if default partition has rows -> create partitions, else skip."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    result = hook.get_first(f"SELECT COUNT(*) FROM {DEFAULT_PARTITION}")
    count = int(result[0]) if result else 0

    context["ti"].xcom_push(key="default_row_count", value=count)
    logger.info("Rows in default partition before run: %s", count)

    if count > 0:
        return "create_partitions"
    return "skip_creation"


def verify_partitions(**context):
    """Verify that rows were moved out of default and list current partitions."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    default_count = hook.get_first(f"SELECT COUNT(*) FROM {DEFAULT_PARTITION}")[0]

    partitions = hook.get_records(
        f"""
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = '{PARTITION_TABLE}'::regclass
        ORDER BY c.relname
        """
    )
    names = [p[0] for p in partitions]

    logger.info("Current partitions (%s): %s", len(names), names)
    logger.info("Rows in default partition after run: %s", default_count)

    context["ti"].xcom_push(key="partition_list", value=names)
    context["ti"].xcom_push(key="remaining_in_default", value=int(default_count))


def collect_partition_stats(**context):
    """Collect row counts by partition and write a summary log record."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    partitions = hook.get_records(
        f"""
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = '{PARTITION_TABLE}'::regclass
        ORDER BY c.relname
        """
    )

    total_rows = 0
    stats = []

    for (partition_name,) in partitions:
        rows = hook.get_first(f"SELECT COUNT(*) FROM public.{partition_name}")[0]
        size = hook.get_first(
            "SELECT pg_size_pretty(pg_total_relation_size(%s::regclass))",
            parameters=(f"public.{partition_name}",),
        )[0]
        stats.append({"partition": partition_name, "rows": int(rows), "size": size})
        total_rows += int(rows)

    logger.info("Partition stats: %s", stats)

    context["ti"].xcom_push(key="partition_stats", value=stats)
    context["ti"].xcom_push(key="total_rows", value=total_rows)

    hook.run(
        """
        INSERT INTO public.partition_log
            (operation_type, rows_affected, status, executed_by)
        VALUES ('STATS_COLLECTION', %s, 'SUCCESS', 'airflow_dag')
        """,
        parameters=(total_rows,),
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_task_failure,
}

with DAG(
    dag_id="dynamic_partition_creation",
    default_args=default_args,
    description="Create partitions for new train types via PostgreSQL stored procedures",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["partitioning", "postgres", "trains"],
    doc_md=__doc__,
) as dag:
    ensure_log_table = SQLExecuteQueryOperator(
        task_id="ensure_log_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS public.partition_log (
                id              BIGSERIAL PRIMARY KEY,
                operation_type  VARCHAR(50)  NOT NULL,
                partition_name  VARCHAR(255),
                train_type      VARCHAR(255),
                rows_affected   BIGINT       DEFAULT 0,
                status          VARCHAR(20)  DEFAULT 'SUCCESS',
                error_message   TEXT,
                executed_by     VARCHAR(100) DEFAULT 'system',
                executed_at     TIMESTAMPTZ  DEFAULT NOW()
            );
        """,
    )

    check_default = BranchPythonOperator(
        task_id="check_for_rows_in_default",
        python_callable=check_for_rows_in_default,
    )

    create_partitions = SQLExecuteQueryOperator(
        task_id="create_partitions",
        conn_id=POSTGRES_CONN_ID,
        sql="CALL public.create_partitions_for_new_types();",
    )

    skip_creation = EmptyOperator(task_id="skip_creation")

    join_branches = EmptyOperator(
        task_id="join_branches",
        trigger_rule="none_failed_min_one_success",
    )

    analyze_parent = SQLExecuteQueryOperator(
        task_id="analyze_parent_table",
        conn_id=POSTGRES_CONN_ID,
        sql="ANALYZE public.trains_with_partition;",
    )

    verify = PythonOperator(
        task_id="verify_partitions",
        python_callable=verify_partitions,
    )

    stats = PythonOperator(
        task_id="collect_partition_stats",
        python_callable=collect_partition_stats,
    )

    ensure_log_table >> check_default
    check_default >> [create_partitions, skip_creation]
    [create_partitions, skip_creation] >> join_branches
    join_branches >> analyze_parent >> verify >> stats

