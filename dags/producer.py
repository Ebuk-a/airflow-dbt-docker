from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")

# with DAG('producer', start_date= datetime(2023,1,1), 
#     schedule= '@daily', catchup= False) as dag:

with DAG(dag_id= "producer",
    schedule= "@daily",
    start_date= datetime(2023, 1, 1),
    catchup= False
    ) as dag:
    
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("first dataset is updated")
    
    @task(outlets=[my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, "a+") as f:
            f.write("second dataset just got updated")

    update_dataset() >> update_dataset_2()
