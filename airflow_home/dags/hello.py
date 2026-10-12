# mncG9EpngF9aDCqg

from airflow.sdk import dag, task
from datetime import datetime

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def hello():
    @task
    def say_hi():
        print("hi from airflow")
        print("this is another print statement")
        print("this is yet another print statement")
    say_hi()

hello()