"""
Ежедневное обновление витрины VT260725214E22__DWH.global_metrics.

Каждый запуск считает один день — логическую дату {{ ds }}: это «вчера» относительно
момента запуска по расписанию. День в витрине пересчитывается целиком
(DELETE + INSERT в одной транзакции), поэтому перезапуск за любой день безопасен.

Перед расчётом DAG ждёт, пока 1_data_import загрузит в staging данные за тот же день.
SQL расчёта — src/sql/datamart_global_metrics.sql.
"""
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.sensors.external_task import ExternalTaskSensor

log = logging.getLogger(__name__)

VERTICA_CONN_ID = 'vertica_dwh'
# в репозитории SQL лежит рядом с папкой dags: src/sql
SQL_FILE = Path(__file__).resolve().parent.parent / 'sql' / 'datamart_global_metrics.sql'


def read_statements(path: Path) -> list:
    text = re.sub(r'--[^\n]*', '', path.read_text(encoding='utf-8'))
    return [s.strip() for s in text.split(';') if s.strip()]


@dag(
    dag_id='2_datamart_update',
    description='Vertica staging → DWH.global_metrics за день',
    schedule_interval='@daily',
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    catchup=True,
    max_active_runs=1,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)},
    tags=['final-project', 'dwh'],
)
def datamart_update():

    # расписания DAG совпадают, поэтому ждём запуск 1_data_import с той же логической датой
    wait_for_staging = ExternalTaskSensor(
        task_id='wait_for_staging',
        external_dag_id='1_data_import',
        external_task_id=None,
        mode='reschedule',
        poke_interval=60,
        timeout=60 * 60 * 6,
    )

    @task
    def update_global_metrics(ds=None):
        day = datetime.strptime(ds, '%Y-%m-%d').date()
        conn = VerticaHook(vertica_conn_id=VERTICA_CONN_ID).get_conn()
        try:
            with conn.cursor() as cur:
                for statement in read_statements(SQL_FILE):
                    cur.execute(statement, {'day': day})
                cur.execute(
                    'SELECT count(*), sum(cnt_transactions) FROM VT260725214E22__DWH.global_metrics '
                    'WHERE date_update = :day',
                    {'day': day},
                )
                currencies, transactions = cur.fetchone()
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        # в источнике есть дни без транзакций (в октябре 2022 данные только за 15 дней) —
        # пустой день не ошибка, строк в витрине за него просто нет
        if not currencies:
            log.warning('global_metrics %s: за день нет выполненных транзакций', ds)
            return
        log.info('global_metrics %s: %s валют, %s транзакций', ds, currencies, transactions)

    wait_for_staging >> update_global_metrics()


datamart_update_dag = datamart_update()
