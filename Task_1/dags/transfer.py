from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

default_args = {
        'owner': 'Andrii',
        'retries': 3,
        'retry_delay': timedelta(seconds=30),
        'execution_timeout': timedelta(seconds=15)
    }

@dag(
    schedule = "@hourly",
    start_date = datetime(2025, 1, 11),
    default_args = default_args,
    catchup = True,
    description = "Transfer data from db1 into db2 and bring all currencies to EUR"
)
def transfer():

    create_table = SQLExecuteQueryOperator(
        task_id = "create_orders_table_if_not_exists",
        conn_id = "postgres_2",
        sql = """
            CREATE TABLE IF NOT EXISTS orders_eur (
                order_id uuid PRIMARY KEY,
                customer_email TEXT,
                order_date TIMESTAMP,
                amount NUMERIC(12, 2),
                currency VARCHAR(3)
            );
        """
    )

    def get_exchange_rates(date: datetime = None, currency: str = "USD") -> dict:
        import requests, json
        
        app_id = Variable.get("exchange_app_id")
        if date:
            log.info(f"Get exchange rates for {currency} on {date}")
            url = f"https://openexchangerates.org/api/historical/{date}.json?app_id={app_id}&base={currency}"
        else:
            log.info(f"Get latest exchange rates for {currency}")
            url = f"https://openexchangerates.org/api/latest.json?app_id={app_id}&base={currency}"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)

        try:
            dict = json.loads(response.text)
            return dict["rates"]
        except json.JSONDecodeError as e:
            log.error("Error parsing json:", e)
        
        return {}
    
    def convert(args: tuple, rates: dict, to_currency: str = "EUR"):
        _, _, order_date, amount, currency = args
        
        if currency == to_currency:
            return amount
        else:
            return round(amount / rates[order_date.date()][currency] * rates[order_date.date()][to_currency], 2)

    @task()
    def transform(data_interval_start, data_interval_end):
        src_hook = PostgresHook(postgres_conn_id='postgres_1')

        sql = """
            SELECT order_id, customer_email, order_date, amount, currency
            FROM orders
            WHERE ingestion_date >= %s
            AND ingestion_date < %s
        """
        
        df = src_hook.get_pandas_df(sql, parameters=[data_interval_start, data_interval_end])

        if df.empty:
            log.info("No new records to transfer.")
            return
        
        log.info(f"Records to transfer: {len(df.index)}")

        order_dates = df['order_date'].dt.date.unique()        
        rates = {d: get_exchange_rates(d) for d in order_dates}

        # initially wrote lambda
        # to_currency = "EUR"
        # l = lambda row: round(row['amount'] / rates[row['order_date'].date()][row['currency']] * rates[row['order_date'].date()][to_currency], 2) if row['currency'] != to_currency else row['amount']
        # df['amount'] = df.apply(l, axis=1)
        
        # but standalone func has move readability to me
        df['amount'] = df.apply(convert, rates = rates, axis = 1)
        df['currency'] = "EUR"

        dst_hook = PostgresHook(postgres_conn_id='postgres_2')
        
        # this approach doesn't handle conflicts, aka UPSERT
        # but it creates table if not exist, so create_table task is not needed
        # df.to_sql('orders_eur', dst_hook.get_sqlalchemy_engine(), index=False, if_exists='append', method='multi')
        
        # this one implements ON CONFLICT, so retries won't crash on duplication indexes.
        dst_hook.insert_rows(
            "orders_eur",
            df.values.tolist(),
            df.columns.values.tolist(),
            5000,
            True,
            executemany = True,
            fast_executemany = True,
            replace_index = "order_id"
        )

        log.info("Task finished successfully")
    
    create_table >> transform()

transfer()