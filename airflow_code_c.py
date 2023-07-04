# 1.python dload data
#2. create cluster
#3. run spark job
#4. delete cluster
#5. postgres hook

#importing the required libraries
from datetime import timedelta, datetime
from airflow import models
import json
import csv
import urllib.request as request
from airflow.operators import PythonOperator
#from airflow.hooks import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule
import os
#import cloudstorage as gcs
#from google.appengine.api import app_identity
import pandas as pd
from pandas.io.json import json_normalize
import gcsfs

#defining a specific start date and the schedule interval for the DAG to run
start_date=datetime(2020,9,2)
schedule_interval=timedelta(days=1)

#fetching the current date's data
def fetch_data(ds, **kwargs):
	#Variable.set('execution_date', kwargs['execution_date'])
	with request.urlopen('https://data.cityofnewyork.us/resource/h9gi-nx95.json?$order=crash_date%20desc&$limit') as response:
		source=response.read()
		data=json.loads(source)
	q_data=[]
	exec_date=datetime.strptime(ds, '%Y-%m-%d')
	exec_minus_4= exec_date-timedelta(days=5)
	exec_minus_4_str=exec_minus_4.strftime('%Y-%m-%d')
	print(exec_minus_4_str)
	for i in range(len(data)):
		#(execution_date - macros.timedelta(days=4)).strftime("%Y-%m-%d")
		date_object = datetime.strptime(data[i]['crash_date'], '%Y-%m-%dT%H:%M:%S.%f')
		string_date= date_object.strftime('%Y-%m-%d')
		if(string_date== exec_minus_4_str):
			print('Yash') #api has data till (T-4)th date
			q_data.append(data[i])

	#print('Equals='+string_date)
	#print('{{macros.ds_add(ds,-5)}}')
	# with open('gs://us-central1-my-project-a5a217ae-bucket/data/data_2b_loaded.json', 'w') as json_file:
	# 	json.dump(q_data, json_file)
	print('Yahoo')
	df=json_normalize(q_data, max_level=1)
	df.to_csv('gs://us-central1-my-project-00fa7678-bucket/data/daysdata.csv')

#sparkstr(
SPARK_CODE = ('gs://us-central1-my-project-00fa7678-bucket/spark_files/spark_job.py')
dataproc_job_name = 'spark_job_dataproc'

#set default arguments to pass
default_args={
	'start_date':start_date,
	'depends_on_past': False,
	'email_on_failure': False,
	'email_on_retry': False,
	'retries':1,
	'retry_delay': timedelta(minutes=5),
	'project_id': models.Variable.get('project_id')
}

#set up a DAG
with models.DAG('pipeline',description='DAG for NYC Collision Stats',
	schedule_interval=schedule_interval,default_args=default_args) as dag:
	
	#execution_date = '{{ execution_date }}'
	#simple bash
	print_date = BashOperator(
	task_id='print_date',
	bash_command='date',
	dag=dag
	)

	#get data
	get_data=PythonOperator(
	task_id='dload_the_results',
	provide_context=True,
	python_callable=fetch_data,
	dag=dag
	)

    #creating a dataproc cluster
	create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
	task_id='create_dataproc_cluster',
	cluster_name='dataproc-cluster-{{ ds_nodash }}',
	num_workers=2,
	zone=models.Variable.get('dataproc_zone'),
	master_machine_type='n1-standard-1',
	worker_machine_type='n1-standard-1',
	dag=dag)

    #run spark job on the cluster
	run_spark = dataproc_operator.DataProcPySparkOperator(
	task_id='run_spark',
	main=SPARK_CODE,
	cluster_name='dataproc-cluster-{{ ds_nodash }}',
	job_name=dataproc_job_name,
	dag=dag
	)   

	#delete the existing dataproc cluster
	delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
	task_id='delete_dataproc',
	cluster_name='dataproc-cluster-{{ ds_nodash }}',
	trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
	dag=dag
	)

	#set dependencies
	print_date >> get_data >> create_dataproc_cluster >> run_spark >> delete_dataproc
	
	
#new error "Insufficient 'IN_USE_ADDRESSES' quota. Requested 3.0, available 1.0.">