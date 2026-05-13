"""
DAG: partition_maintenance
Description:
- weekly health check of partitions,
- archive old partitions via stored procedure,
- cleanup old log records,
- produce summary report.
Schedule: every Sunday at 03:00.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_default"
PARTITION_TABLE = "public.trains_with_partition"
ARCHIVE_THRESHOLD_DAYS = 365
LOG_RETENTION_DAYS = 90


def on_task_failure(context):
    ti = context["task_instance"]
    logger.error("Task '%s' in DAG '%s' failed", ti.task_id, context["dag"].dag_id)


def partition_health_check(**context):
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

    report = []
    issues = []

    for (partition_name,) in partitions:
        try:
            rows = hook.get_first(f"SELECT COUNT(*) FROM public.{partition_name}")[0]
            report.append({"partition": partition_name, "rows": int(rows), "status": "OK"})
            if int(rows) == 0 and partition_name != "trains_default":
                issues.append(f"Empty partition: {partition_name}")
        except Exception as exc:
            report.append({"partition": partition_name, "rows": -1, "status": "ERROR"})
            issues.append(f"Read error for {partition_name}: {exc}")

    logger.info("Health report: %s", report)
    if issues:
        logger.warning("Detected issues: %s", issues)

    context["ti"].xcom_push(key="health_report", value=report)
    context["ti"].xcom_push(key="issues", value=issues)


def generate_report(**context):
    """Generate final maintenance summary from XCom and partition_log."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    health = context["ti"].xcom_pull(task_ids="partition_health_check", key="health_report") or []
    issues = context["ti"].xcom_pull(task_ids="partition_health_check", key="issues") or []

    archive_count = hook.get_first(
        """
        SELECT COUNT(*)
        FROM public.partition_log
        WHERE operation_type = 'ARCHIVE_PARTITION'
          AND status = 'SUCCESS'
          AND executed_at >= NOW() - interval '8 days'
        """
    )[0]

    logger.info("=" * 60)
    logger.info("PARTITION MAINTENANCE SUMMARY")
    logger.info("Checked partitions: %s", len(health))
    logger.info("Detected issues: %s", len(issues))
    logger.info("Archived partitions in last 8 days: %s", int(archive_count))
    logger.info("=" * 60)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}

with DAG(
    dag_id="partition_maintenance",
    default_args=default_args,
    description="Maintenance and archival of partitions via PostgreSQL stored procedures",
    schedule_interval="0 3 * * 0",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["partitioning", "maintenance", "postgres"],
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

    health_check = PythonOperator(
        task_id="partition_health_check",
        python_callable=partition_health_check,
    )

    archive_old = SQLExecuteQueryOperator(
        task_id="archive_old_partitions",
        conn_id=POSTGRES_CONN_ID,
        sql=f"CALL public.archive_old_partitions({ARCHIVE_THRESHOLD_DAYS});",
    )

    cleanup_old_logs = SQLExecuteQueryOperator(
        task_id="cleanup_old_logs",
        conn_id=POSTGRES_CONN_ID,
        sql=f"CALL public.cleanup_partition_logs({LOG_RETENTION_DAYS});",
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    ensure_log_table >> health_check >> archive_old >> cleanup_old_logs >> report

