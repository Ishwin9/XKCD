from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

import io
import requests
from datetime import datetime
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# Replace with your project & dataset
PROJECT_ID = "xkcd-487501"
DATASET_ID = "xkcd_dataset"
TABLE_ID = "comics"

BASE_URL = "https://xkcd.com"

default_args = {
    "owner": "airflow",
    "retries": 1
}

def load_to_bigquery(combined_json):
    
    key_dict = {"type": "service_account","project_id": "xkcd-487501","private_key_id": "8c4d1de426f54cb58616c50acb26502f45552a27","private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDpT8YoyD4rSr3F\nKVNTEc+cGVjEEsZxhqDzjQXwQpM4JjBWAjeIvkoYukbtzd2nve8D1e6CzonVs0ui\nIcnYcjpfY3lB5Gbx2Q3OB2ZZksQVNiEGX1MNxludZ2BmFGbjtnAo+3vgq/UPj9+a\nf/7IZ9RYoTf8V//uVUSoa7X98wGemN9q0uRpp+53R3ckzQc8tQ4QRyYMsAM7T7Pq\nsMgQeRo6I9h2l9aoKuGiRV1qWpU4/zTaYNfCBVs/JRz8klvPHJM0NnXQuoXgOVvS\nUwbpIlYNApiq/FSigRLdNBSts8L1ut1kmVBwSQPIOF0xq4V+u6v/0pJxL28lfZPm\nFPzp3T5NAgMBAAECggEAAgnUTiJpEqPsR7GeUf/k67T3/Cc61x6T6j99lrVfh70j\nrSNcp1lG7fOLWkQIb1D+6oO/D02tJsdn04KOkCbNVkHY+xz6/kVvyVtHXGR7yetS\nELYaqb3lf9j5nrgHvxkRXqD12ocTah+ka+XkN2La2CxjwBS9E8YTG71B1uES5Exm\n2qQSJoBu4vlpGRVyV0B1+OAwa3dRBEFauwmvr5Qk0IoPfQgZ25+2pI/zcurdzjKt\naCs7AfJPt4lQhKkpjjYF/hzwWV7LU/YHRfF2D6uAyXcsVRV6ZcpA/UScyYgezcCY\nuk5VljjUJi2nAwkefQxwHlub5V/j2xyoghN0Ll6pyQKBgQD3gXyv0xZGYy/D1/uC\nvvpsudR0iRH9bYPqlX7GpwC8n1o2o6XQpSfcfD2Wdv9iqs157s3qYSjkoEGHi2gu\nmmCu4OQLCr3V/2KBb8xQQa2isOpgLqGgMvV1OdXpIium5qFVpMupUePrC6443S6T\nTMEx3f20Gvz/JFUH4W54cUSzxQKBgQDxUZS+QuywzQ7Db3LN1odzBkVnj+Gd0mYw\nqQaclcWJmVpnjGnIb3mt9AaBX8kCUJO/Flv3nqQ0hLhD+EGW9rOHkQO+ugAZw2Um\nCAiOSEUkMO4sE18SgRA9w3BTyzWxoedaQ3ILw8irpEyBsGtsjNivDitXuxNff5GP\nmhwGsTkg6QKBgBHvWyauiCWvmE17wCj4R9NLH/8V06zmm7GLBbXFckM41OOythhJ\nb3sFsbzOgLEYqW0VhexfAASZLEZzLqh53VmuyDeYnqr8J5ozjL2gFMH+Se2QU3v1\nVV4aQ8ryA/HDMpuvJmbtnTNyFDzgojhgTnubl3/OmNiwPE0m48dshcr1AoGAFUlh\n+oqCwHHGLJ0nHlsfJZlXA/SgLUDC/OXcDHH1s9aQL/Ql3KVgsWSGMmFVpNugvMln\nIWegCnXunyhF+OAYJAw5rVhQ6/TddkZdItfjKXcbe03WJBa4bQpRXULeoKWsd7zd\nUYZarDFZAlOiljeyYXa5ggqhgdoJ6AXrFPgLFwkCgYEAqeIN6KtrzXlmPTG5/FDB\niKiV6n1cTy2hC7oM37IEIAyioz5i+31jRa2qB5b9YJ7aZQnKNQZfXIX1pTZFe5Cc\nHiH0+dP35vpJ8moKubhu9GN9lVl1KMv1Uemtn9iXbpe/qC+OqtCzCZlEaHzbGH1g\n36/2cvvsRNsPO7XqosCnwXQ=\n-----END PRIVATE KEY-----\n","client_email": "xkcd-ingestion@xkcd-487501.iam.gserviceaccount.com","client_id": "118214397705396665610","auth_uri": "https://accounts.google.com/o/oauth2/auth","token_uri": "https://oauth2.googleapis.com/token","auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xkcd-ingestion%40xkcd-487501.iam.gserviceaccount.com","universe_domain": "googleapis.com"}
    credentials = service_account.Credentials.from_service_account_info(key_dict)

    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Convert rows to JSON lines
    json_data = "\n".join([json.dumps(r) for r in combined_json])
    file_obj = io.StringIO(json_data)

    # Load into BigQuery (batch)
    table_id = "xkcd-487501.xkcd_dataset.comics"

    job = client.load_table_from_file(file_obj, table_id, job_config=bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    ))
    job.result() 
    print(f"Loaded {len(combined_json)} rows into {table_id}")

def get_latest_comic_number():
    r = requests.get(f"{BASE_URL}/info.0.json")
    r.raise_for_status()
    print(f"Latest comic number: {r.json()['num']}")
    return r.json()["num"]


def check_first_run():
    stored = Variable.get("xkcd_last_comic", default_var=None)
    if stored is None:
        return "full_load"
    return "wait_for_new_comic"

def full_load():
    latest = get_latest_comic_number()
    combined_data = []

    print("Running FULL LOAD")

    for comic_id in range(500,550):
        print(f"Processing comic ID: {comic_id}")
        r = requests.get(f"{BASE_URL}/{comic_id}/info.0.json")
        if r.status_code == 404:
            continue

        data = r.json()
        combined_data.append(r.json())

        print(f"Total comics fetched: {len(combined_data)}")
    
    load_to_bigquery(combined_data)

    Variable.set("xkcd_last_comic", latest)
    print(f"Watermark stored: {latest}")


def wait_for_new_comic():
    stored = int(Variable.get("xkcd_last_comic"))
    latest = get_latest_comic_number()

    print(f"Stored: {stored}, Latest: {latest}")

    return latest > stored

def incremental_load():
    stored = int(Variable.get("xkcd_last_comic"))
    latest = get_latest_comic_number()
    combined_data = []

    print(f"Ingesting comics {stored+1} to {latest}")

    for comic_id in range(stored + 1, latest + 1):
        r = requests.get(f"{BASE_URL}/{comic_id}/info.0.json")

        if r.status_code == 404:
            continue

        r.raise_for_status()
        data = r.json()
        combined_data.append(data)

        print(f"New comics fetched: {len(combined_data)}")

        if combined_data:
            load_to_bigquery(combined_data)

        print(f"Ingested comic {data['num']} - {data['title']}")

    Variable.set("xkcd_last_comic", latest)
    print(f"Watermark updated to {latest}")

def trigger_dbt_job():
    account_id = 70471823534282
    job_id = 70471823563495
    api_token = "dbtu_k4MkDn0BSOd7n13bdgc0q9zZiQDTXB63OS1OTrf_ocRSfu3LqI"
    url = f"https://rf779.us1.dbt.com/api/v2/accounts/70471823534282/jobs/70471823563495/run/"
    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json={})

    payload = {
    "cause": "Triggered from Airflow"
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:
        raise Exception(f"Failed to trigger dbt job: {response.text}")

    return True

with DAG(
    dag_id="xkcd_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * 1,3,5",
    catchup=False,
) as dag:

    start = BranchPythonOperator(
        task_id="check_if_first_run",
        python_callable=check_first_run,
    )

    full_load_task = PythonOperator(
        task_id="full_load",
        python_callable=full_load,
    )

    wait_sensor = PythonSensor(
        task_id="wait_for_new_comic",
        python_callable=wait_for_new_comic,
        poke_interval=600,
        timeout=86400,
        mode="poke"
    )

    incremental_task = PythonOperator(
        task_id="incremental_load",
        python_callable=incremental_load,
    )

    end = EmptyOperator(task_id="end",trigger_rule="none_failed_min_one_success")

    trigger_dbt = PythonOperator(
        task_id="trigger_dbt",
        python_callable=trigger_dbt_job,
        trigger_rule="none_failed_min_one_success"
    )

    start >> full_load_task
    full_load_task >> end >> trigger_dbt
    start >> wait_sensor >> incremental_task
    incremental_task >> end >> trigger_dbt
