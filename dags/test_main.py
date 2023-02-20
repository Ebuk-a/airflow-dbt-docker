from airflow.decorators import dag, task 
from datetime import datetime

@dag(start_date= datetime(2023, 1 , 1), catchup=False)
def dag() :

    @task 
    def start():
        return 42

    @task
    def print (val):
        print(val)

    print(start())

if __name__== "__main__":
    dag() .test ()