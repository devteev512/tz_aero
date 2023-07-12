import requests
import logging
import psycopg
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class CannApiOperator(BaseOperator):
    url = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'


    def __init__(self, **kwargs):
        super().__init__(**kwargs)


    def connect_to_db(self):
        try:
            conn_string = 'postgresql://postgres:postgres@postgres:5432/postgres'
            conn = psycopg.connect(conn_string)
            return conn
        except Exception as e:
            logging.exception(e)
            raise


    def prepare_db_table(self, cursor):
        cursor.execute(f" \
        CREATE TABLE IF NOT EXISTS public.cann_data  \
            (id int PRIMARY KEY, \
             uid text,  \
             strain text, \
             cannabinoid_abbreviation text, \
             cannabinoid text,  \
             terpene text, \
             medical_use text,    \
             health_benefit text, \
             category text, \
             type text, \
             buzzword text, \
             brand text)")
        cursor.execute(f"truncate table public.cann_data")


    def get_data(self, url):
        try:
            response = requests.get(url)
            logging.info(f"HTTP STATUS {response.status_code}")
            data = response.json()
            return data
        except Exception as e:
            logging.exception(e)
            raise 


    def execute(self, context):
        conn = self.connect_to_db()
        cursor = conn.cursor()
        self.prepare_db_table(cursor)
        sql = 'copy cann_data (id, uid, strain, cannabinoid_abbreviation, cannabinoid, terpene, \
        medical_use, health_benefit, category, type, buzzword, brand) from stdin '
        with cursor.copy(sql) as copy:
            for record in self.get_data(self.url):
                logging.info(record)
                try:
                    copy.write_row((record['id'], record['uid'], record['strain'], record['cannabinoid_abbreviation'], record['cannabinoid'], 
                                record['terpene'], record['medica_use'], record['health_benefit'], record['category'], record['type'],  
                                record['buzzword'],  record['brand']))
                except KeyError as e:
                    logging.warning(f"Got a KeyError with key {e}")
                    pass
        logging.info("DATA LOADED INTO TABLE CANN_DATA")
        conn.commit()
        conn.close()
