import string
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task

# Defining DAG (via taskflow api)

@dag(
    dag_id='taskflow_api_example',
    is_paused_upon_creation=False,
    catchup=False,
    tags=["example"],
    max_active_runs=1,
    start_date= datetime(2023,12,31,7),
    default_args={
        'depends_on_past': False,
        'retry_delay': timedelta(seconds=5),
        'schedule_interval':'*/5 * * * *',
        #'end_date': datetime(2024,9,30,8)
    },
)
def taskflow_api_test():
    @task()
    def remove_special_chars(text: str) -> str:
        allowed_chars = set(string.ascii_letters)
        non_special_text = ''.join(char for char in text if char in allowed_chars)
        return non_special_text

    @task()
    def count_letters(text: str) -> int:
        return len(text)

    @task()
    def report_result(text: str, length: int) -> str:
        return f"The word \"{text}\" has {length} non-special characters in it."

    special_chars_removed = remove_special_chars("!!!Supercalifrag-il+_isticexpialidocious?")
    num_letters = count_letters(special_chars_removed)
    sentence = report_result(special_chars_removed, report_result)

taskflow_api_test()
