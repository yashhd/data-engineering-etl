# Creating an ETL Pipeline for generating statistics

Developed an ETL (Extract, Transform, Load) pipeline that utilizes the NYC Motor Vehicle Collisions API to extract data. The pipeline then generates statistics regarding the number of people injured or killed in accidents, as well as the specific locations where these incidents occurred on a given day.

To accomplish this, I wrote a script that effectively retrieves data from the API and stores it in Google Cloud Storage (GCS). The pipeline leverages Spark code to execute a PySpark job within the Dataproc cluster. To ensure seamless orchestration and task execution, I integrated Airflow, which runs a DAG (Directed Acyclic Graph) that ensures all the tasks are executed in the desired sequence.
