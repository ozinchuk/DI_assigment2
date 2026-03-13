import logging
import os
import json
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

DUCKDB_PATH = '/usr/local/airflow/include/analytics.duckdb'
JSON_DIR = '/usr/local/airflow/include/telephony_data'


def task_failure_alert(context):
    ti = context.get('task_instance')
    logging.error(f"ALERT: Task {ti.task_id} failed in DAG {ti.dag_id}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_alert,
}

with DAG(
        'telephony_analytics_etl',
        default_args=default_args,
        schedule='@hourly',
        catchup=False
) as dag:
    def detect_new_calls(**context):
        watermark = Variable.get("last_loaded_call_time", default_var="1970-01-01 00:00:00")

        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
        sql = f"SELECT * FROM calls WHERE call_time > '{watermark}'"
        df = mysql_hook.get_pandas_df(sql)

        logging.info(f"Detected {len(df)} new calls.")

        if df.empty:
            return None

        return df.to_json()


    def load_telephony_details(**context):
        ti = context['ti']
        new_calls_json = ti.xcom_pull(task_ids='detect_new_calls')

        if not new_calls_json:
            return None

        calls_df = pd.read_json(new_calls_json)
        call_ids = calls_df['call_id'].astype(str).tolist()

        telephony_records = []
        rejected_files = 0

        for call_id in call_ids:
            file_path = os.path.join(JSON_DIR, f"call_{call_id}.json")
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    try:
                        data = json.load(f)
                        if all(k in data for k in ("call_id", "duration_sec", "short_description")):
                            if int(data["duration_sec"]) >= 0:
                                telephony_records.append(data)
                            else:
                                rejected_files += 1
                                logging.warning(f"Rejected {file_path}: negative duration.")
                        else:
                            rejected_files += 1
                            logging.warning(f"Rejected {file_path}: missing fields.")
                    except json.JSONDecodeError:
                        rejected_files += 1
                        logging.warning(f"Rejected {file_path}: invalid JSON.")
            else:
                logging.warning(f"Missing JSON for call_id {call_id}.")

        logging.info(f"Loaded {len(telephony_records)} JSONs. Rejected: {rejected_files}.")
        return telephony_records


    def transform_and_load_duckdb(**context):
        ti = context['ti']
        new_calls_json = ti.xcom_pull(task_ids='detect_new_calls')
        telephony_data = ti.xcom_pull(task_ids='load_telephony_details')

        if not new_calls_json or not telephony_data:
            logging.info("No data. Skipping load.")
            return

        calls_df = pd.read_json(new_calls_json)
        telephony_df = pd.DataFrame(telephony_data)

        initial_len = len(telephony_df)
        telephony_df = telephony_df.drop_duplicates(subset=['call_id'])
        if len(telephony_df) < initial_len:
            logging.warning(f"Dropped {initial_len - len(telephony_df)} duplicates.")

        mysql_hook = MySqlHook(mysql_conn_id='mysql_conn')
        employees_df = mysql_hook.get_pandas_df("SELECT * FROM employees")

        enriched_df = calls_df.merge(employees_df, left_on='employee_id', right_on='id', how='inner')
        final_df = enriched_df.merge(telephony_df, on='call_id', how='inner')

        logging.info(f"Final dataset size: {len(final_df)} rows.")

        if final_df.empty:
            logging.info("Dataset empty after joins. Skipping.")
            return

        con = duckdb.connect(DUCKDB_PATH)

        tables = con.execute("SHOW TABLES").df()
        if 'support_call_enriched' not in tables['name'].values:
            con.execute("CREATE TABLE support_call_enriched AS SELECT * FROM final_df")
            logging.info("Created table support_call_enriched.")
        else:
            con.register('final_df_view', final_df)
            con.execute("""
                DELETE FROM support_call_enriched 
                WHERE call_id IN (SELECT call_id FROM final_df_view)
            """)
            con.execute("INSERT INTO support_call_enriched SELECT * FROM final_df_view")
            logging.info("Upserted data.")

        latest_call_time = calls_df['call_time'].max()
        Variable.set("last_loaded_call_time", str(latest_call_time))
        logging.info(f"Watermark updated to {latest_call_time}.")
        con.close()


    t1 = PythonOperator(task_id='detect_new_calls', python_callable=detect_new_calls)
    t2 = PythonOperator(task_id='load_telephony_details', python_callable=load_telephony_details)
    t3 = PythonOperator(task_id='transform_and_load_duckdb', python_callable=transform_and_load_duckdb)

    t1 >> t2 >> t3