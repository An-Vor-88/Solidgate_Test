from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

CURRENCIES = ['AED','AFN','ALL','AMD','ANG','AOA','ARS','AUD','AWG','AZN','BAM','BBD','BDT','BGN','BHD','BIF','BMD','BND','BOB',
              'BRL','BSD','BTC','BTN','BWP','BYN','BZD','CAD','CDF','CHF','CLF','CLP','CNH','CNY','COP','CRC','CUC','CUP','CVE',
              'CZK','DJF','DKK','DOP','DZD','EGP','ERN','ETB','EUR','FJD','FKP','GBP','GEL','GGP','GHS','GIP','GMD','GNF','GTQ',
              'GYD','HKD','HNL','HRK','HTG','HUF','IDR','ILS','IMP','INR','IQD','IRR','ISK','JEP','JMD','JOD','JPY','KES','KGS',
              'KHR','KMF','KPW','KRW','KWD','KYD','KZT','LAK','LBP','LKR','LRD','LSL','LYD','MAD','MDL','MGA','MKD','MMK','MNT',
              'MOP','MRU','MUR','MVR','MWK','MXN','MYR','MZN','NAD','NGN','NIO','NOK','NPR','NZD','OMR','PAB','PEN','PGK','PHP',
              'PKR','PLN','PYG','QAR','RON','RSD','RUB','RWF','SAR','SBD','SCR','SDG','SEK','SGD','SHP','SLL','SOS','SRD','SSP',
              'STD','STN','SVC','SYP','SZL','THB','TJS','TMT','TND','TOP','TRY','TTD','TWD','TZS','UAH','UGX','USD','UYU','UZS',
              'VES','VND','VUV','WST','XAF','XAG','XAU','XCD','XDR','XOF','XPD','XPF','XPT','YER','ZAR','ZMW','ZWL']

@dag(
    schedule = "*/10 * * * *",
    start_date = datetime(2025, 5, 11),
    params = {"volume": 5000},
    catchup = False,
    description = "Create order table in db1 and populate it with data"
)
def generate():
    
    create_table = SQLExecuteQueryOperator(
        task_id = "create_orders_table_if_not_exists",
        conn_id = "postgres_1",
        sql = """
            CREATE TABLE IF NOT EXISTS orders (
                order_id uuid PRIMARY KEY,
                customer_email TEXT,
                order_date TIMESTAMP,
                amount NUMERIC(10, 2),
                currency VARCHAR(3),
                ingestion_date TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS orders_ingestion_date_index
            ON orders (ingestion_date);
        """
    )

    def get_currencies() -> list[str]:
        import requests, json

        log.info("Get currencies list")
        
        app_id = Variable.get("exchange_app_id")
        url = f"https://openexchangerates.org/api/currencies.json?app_id={app_id}"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        
        try:
            dict = json.loads(response.text)            
            # VEF was deprecated in 2018 in favor of VES but for some reason is present in list.
            # I've got error as it is not present in rates for obvious reason.
            # So just removing it from generator, since we don't generate orders for that period.
            dict.pop('VEF')
            return [*dict]
        except json.JSONDecodeError as e:
            log.error("Error parsing json:", e)
        
        return []


    def generate_records(num: int, data_interval_end: datetime):
        import uuid, random, string
        
        # from faker import Faker
        # fake = Faker()

        records = []
        for _ in range(num):
            order_id = str(uuid.uuid4())
            
            email_name = ''.join(random.choices(string.ascii_lowercase, k = random.randint(3, 10)))
            email = email_name + "@test.com"
            order_date = data_interval_end - timedelta(days = random.randint(0, 6), hours = random.randint(0, 23), minutes = random.randint(0, 59))
            amount = round(random.uniform(1, 10000), 2)
            currency = random.choice(CURRENCIES) # get_currencies() or CURRENCIES
            ingestion_date = data_interval_end

            records.append((order_id, email, order_date, amount, currency, ingestion_date))

        log.info(f"Generated {len(records)}.")
        
        return records

    @task()
    def populate(params: dict, data_interval_end: datetime):
        log.info("Get postgres hook")
        dst_hook = PostgresHook(postgres_conn_id='postgres_1')

        log.info("Generate records")
        records = generate_records(params["volume"], data_interval_end)

        dst_hook.insert_rows(
            "orders",
            records,
            ("order_id", "customer_email", "order_date", "amount", "currency", "ingestion_date"),
            5000,
            executemany = True,
            fast_executemany = True
        )
        log.info("Task finished successfully")

    create_table >> populate()

generate()