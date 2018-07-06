

# prelim

import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator 

def print_world():
    print('world')

default_args = {
    'owner' : 'Phil'
    , 'start_date' : dt.datetime(2018, 6, 24)
    , 'retries' : 1
    , 'retry_delay' : dt.timedelta(minutes=5)
}

with DAG('pyOhio_demo'
        , default_args=default_args
        , schedule_interval='0 * * * *'
        ) as dag:

    cf_prog_1 = BashOperator(task_id='cf_prog_1'
                            , bash_command='echo cf1'
                            )

    cf_prog_2 = BashOperator(task_id='cf_prog_2'
                            , bash_command='echo cf2' 
                            )

    cf_prog_3 = BashOperator(task_id='cf_prog_3'
                            , bash_command='echo cf3' 
                            )

    sd_prog_1 = BashOperator(task_id='sd_prog_1'
                            , bash_command='echo sd1'
                            )
    
    sd_prog_2 = BashOperator(task_id='sd_prog_2'
                            , bash_command='echo sd2'
                            )

    sd_prog_3 = BashOperator(task_id='sd_prog_3'
                            , bash_command='echo sd3'
                            )

    sd_prog_4 = BashOperator(task_id='sd_prog_4'
                            , bash_command='echo sd4'
                            )
    
    
    ensemble_prog_1 = BashOperator(task_id='ensemble_prog_1'
                            , bash_command='echo en1'
                            )
    
    kafka_prog_1 = BashOperator(task_id='kafka_prog_1'
                            , bash_command='echo kaf1' 
                            )


cf_prog_2.set_upstream(cf_prog_1)
cf_prog_3.set_upstream(cf_prog_2)

sd_prog_2.set_upstream(sd_prog_1)
sd_prog_3.set_upstream(sd_prog_2)
sd_prog_4.set_upstream(sd_prog_3)

cf_prog_3.set_downstream(ensemble_prog_1)
sd_prog_4.set_downstream(ensemble_prog_1)

kafka_prog_1.set_upstream(ensemble_prog_1)


