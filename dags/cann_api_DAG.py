from airflow import DAG
from airflow.utils.dates import days_ago
from cann_api_connector.connector import CannApiOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-evteev',
    'poke_interval': 600
}

with DAG("load_cann_api",
         schedule_interval='0 */12 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-evteev'],
         ) as dag:
    start = DummyOperator(task_id="start")


    load_cann_data = CannApiOperator(
        task_id='load_cann_data',
    )
        
    end = DummyOperator(task_id="end")


    start >> load_cann_data >> end
