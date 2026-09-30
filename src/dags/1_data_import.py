"""
Ежедневная загрузка сырых данных из PostgreSQL (public.transactions, public.currencies)
в staging-слой Vertica.

Каждый запуск обрабатывает один день — логическую дату запуска {{ ds }}:
данные этого дня в staging удаляются и загружаются заново в одной транзакции,
поэтому перезапуск за любой день безопасен и не создаёт дублей.

Подключения Airflow:
    postgres_source — PostgreSQL-источник (db1);
    vertica_dwh     — Vertica.
"""
import csv
import io
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook

log = logging.getLogger(__name__)

PG_CONN_ID = 'postgres_source'
VERTICA_CONN_ID = 'vertica_dwh'
STAGING_SCHEMA = 'VT260725214E22__STAGING'

TRANSACTIONS_COLUMNS = [
    'operation_id', 'account_number_from', 'account_number_to', 'currency_code', 'country',
    'status', 'transaction_type', 'amount', 'transaction_dt',
]
CURRENCIES_COLUMNS = ['date_update', 'currency_code', 'currency_code_with', 'currency_code_div']

# DISTINCT — в источнике встречаются полные дубли строк
TRANSACTIONS_SQL = """
    SELECT DISTINCT
        operation_id, account_number_from, account_number_to, currency_code, country,
        status, transaction_type, amount, transaction_dt
    FROM public.transactions
    WHERE transaction_dt >= %(day_start)s AND transaction_dt < %(day_end)s
"""

# в источнике колонка курса называется currency_with_div
CURRENCIES_SQL = """
    SELECT DISTINCT
        date_update, currency_code, currency_code_with, currency_with_div AS currency_code_div
    FROM public.currencies
    WHERE date_update >= %(day_start)s AND date_update < %(day_end)s
"""


def load_day(table: str, source_sql: str, columns: list, date_column: str, ds: str) -> None:
    day_start = datetime.strptime(ds, '%Y-%m-%d')
    day_end = day_start + timedelta(days=1)

    # 1. выгрузка дня из источника в CSV в памяти (за день — десятки тысяч строк)
    with PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(source_sql, {'day_start': day_start, 'day_end': day_end})
            rows = cur.fetchall()

    buffer = io.StringIO()
    csv.writer(buffer).writerows(rows)
    buffer.seek(0)

    # 2. замена дня в staging: DELETE + COPY и один commit в конце
    target = f'{STAGING_SCHEMA}.{table}'
    vertica_conn = VerticaHook(vertica_conn_id=VERTICA_CONN_ID).get_conn()
    try:
        with vertica_conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM {target} WHERE {date_column} >= :day_start AND {date_column} < :day_end',
                {'day_start': day_start, 'day_end': day_end},
            )
            cur.copy(
                f"COPY {target} ({', '.join(columns)}) FROM STDIN "
                f"DELIMITER ',' ENCLOSED BY '\"' ABORT ON ERROR NO COMMIT",
                buffer,
            )
            cur.execute(
                f'SELECT count(*) FROM {target} WHERE {date_column} >= :day_start AND {date_column} < :day_end',
                {'day_start': day_start, 'day_end': day_end},
            )
            loaded = cur.fetchone()[0]
        vertica_conn.commit()
    except Exception:
        vertica_conn.rollback()
        raise
    finally:
        vertica_conn.close()

    if loaded != len(rows):
        raise ValueError(f'{target} {ds}: выгружено {len(rows)} строк, в staging {loaded}')
    log.info('%s %s: загружено %s строк', target, ds, loaded)


@dag(
    dag_id='1_data_import',
    description='PostgreSQL → Vertica staging: transactions и currencies за день',
    schedule_interval='@daily',
    # период проекта — октябрь 2022; каждый день отрабатывает отдельным запуском
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    catchup=True,
    max_active_runs=1,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)},
    tags=['final-project', 'staging'],
)
def data_import():

    @task
    def load_transactions(ds=None):
        load_day('transactions', TRANSACTIONS_SQL, TRANSACTIONS_COLUMNS, 'transaction_dt', ds)

    @task
    def load_currencies(ds=None):
        load_day('currencies', CURRENCIES_SQL, CURRENCIES_COLUMNS, 'date_update', ds)

    load_transactions()
    load_currencies()


data_import_dag = data_import()
