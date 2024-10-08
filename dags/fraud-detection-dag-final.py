from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.trigger_rule import TriggerRule
import smtplib
from email.message import EmailMessage
import json
import pandas as pd
import numpy as np
from datetime import datetime
from geopy.distance import geodesic
import joblib
import requests
import os
import logging


# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Chemins des fichiers
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, "model.joblib")
PREPROCESSOR_PATH = os.path.join(BASE_DIR, "scaler.joblib")

# Chargement du modèle et du préprocesseur
model = joblib.load(MODEL_PATH)
preprocessor = joblib.load(PREPROCESSOR_PATH)

def calculate_age(born):
    today = datetime.now()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def preprocess_dataframe(df):
    df['current_time'] = pd.to_datetime(df['current_time'], unit='ms')
    df['dob'] = pd.to_datetime(df['dob'])
    df['age'] = df['dob'].apply(calculate_age)
    df['hour'] = df['current_time'].dt.hour
    df['dayofweek'] = df['current_time'].dt.dayofweek
    df['month'] = df['current_time'].dt.month
    df['dayofyear'] = df['current_time'].dt.dayofyear
    df['dayofmonth'] = df['current_time'].dt.day
    df['weekofyear'] = df['current_time'].dt.isocalendar().week
    df['distance'] = df.apply(lambda row: geodesic((row['lat'], row['long']), 
                                                   (row['merch_lat'], row['merch_long'])).km, axis=1)
    df['card_issuer_Bank'] = df['cc_num'].astype(str).str[1:6].astype(int)
    df['card_issuer_MMI'] = 'mmi' + df['cc_num'].astype(str).str[0]
    return df

def predict(data):
    df = pd.DataFrame([data])
    df = preprocess_dataframe(df)
    
    features = ['amt', 'lat', 'long', 'city_pop', 'merch_lat', 'merch_long', 'age', 
                'hour', 'dayofweek', 'month', 'dayofyear', 'dayofmonth', 'weekofyear', 
                'card_issuer_Bank', 'distance', 'category', 'gender', 'card_issuer_MMI']
    
    df = df[features]
    X_processed = preprocessor.transform(df)
    
    return model.predict(X_processed)[0] 

def get_transaction(**kwargs):
    url = "https://real-time-payments-api.herokuapp.com/current-transactions"
    response = requests.get(url)
    response.raise_for_status()
    transactions = response.json()
    kwargs['ti'].xcom_push(key='transactions', value=transactions)

def analyze_and_branch(**context):
    ti = context['ti']
    transactions_json = ti.xcom_pull(task_ids='get_transaction_task', key='transactions')
    
    df = pd.DataFrame(json.loads(transactions_json)['data'], 
                      columns=json.loads(transactions_json)['columns'])
    
    df = preprocess_dataframe(df)
    df['predicted_fraud'] = df.apply(lambda row: predict(row.to_dict()), axis=1)
    
    ti.xcom_push(key='analyzed_transactions', value=df.to_json())
    
    if df['predicted_fraud'].any():
        return 'handle_fraud_task'
    else:
        return 'no_fraud_task'


def handle_fraud(**context):
    ti = context['ti']
    transactions_json = ti.xcom_pull(task_ids='analyze_transaction_task', key='analyzed_transactions')
    df = pd.read_json(transactions_json)
    fraud_transactions = df[df['predicted_fraud'] == 1]
    if not fraud_transactions.empty:
        logging.info(f"Fraude détectée dans {len(fraud_transactions)} transactions.")
        ti.xcom_push(key='fraud_transactions', value=fraud_transactions.to_json())
    else:
        logging.info("Aucune transaction frauduleuse à signaler.")


def no_fraud(**context):
    logging.info("Aucune fraude détectée")

# Ajoutez ces nouvelles fonctions
def send_fraud_email(**context):
    ti = context['ti']
    fraud_transactions_json = ti.xcom_pull(task_ids='handle_fraud_task', key='fraud_transactions')
    if fraud_transactions_json:
        fraud_transactions = pd.read_json(fraud_transactions_json)
        body = f"Fraude détectée dans {len(fraud_transactions)} transactions:\n\n{fraud_transactions.to_string()}"
    else:
        body = "Fraude détectée, mais aucun détail disponible."
    send_fraud_alert_email("Alerte de fraude", body, 'xxxxx@gmail.com')

def send_no_fraud_email(**context):
    send_fraud_alert_email("Aucune fraude détectée", "Aucune transaction frauduleuse n'a été identifiée.", 'xxxxxxxx@gmail.com')
    

def save_to_s3(**context):
    ti = context['ti']
    transaction = ti.xcom_pull(task_ids='analyze_transaction_task', key='analyzed_transactions')
    
    execution_date = context['execution_date']
    timestamp = execution_date.strftime("%Y%m%d_%H%M%S")
    filename = f"transaction_{timestamp}.json"
    s3_key = f"transactions/{filename}"
    
    try:
        with open(f"/tmp/{filename}", 'w') as f:
            json.dump(transaction, f, indent=2)
        
        s3_hook = S3Hook(aws_conn_id="aws_default")
        
        s3_hook.load_file(
            filename=f"/tmp/{filename}",
            key=s3_key,
            bucket_name=Variable.get("S3BucketName"),
            replace=True
        )
        logging.info(f"Transaction sauvegardée sur S3 : {s3_key}")
        
        # Stocker le chemin S3 pour l'utiliser plus tard
        ti.xcom_push(key='s3_file_path', value=s3_key)
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde sur S3: {e}")
        raise

# Fonction qui envoie l'e-mail avec smtplib
def send_fraud_alert_email(subject, body, to_email):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587  # Port pour TLS
    smtp_user = "xxxxxx@gmail.com"
    smtp_password = "xxxxxxx"

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = smtp_user
    msg['To'] = to_email
    
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Démarre la connexion sécurisée TLS
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            print(f"Email envoyé à {to_email} avec succès !")

    except Exception as e:
        print(f"Erreur lors de l'envoi de l'e-mail : {e}")

# Définissez la requête SQL pour créer la table Redshift
create_table_sql = """
CREATE TABLE IF NOT EXISTS fraud_transactions (
    cc_num BIGINT,
    merchant VARCHAR(255),
    category VARCHAR(50),
    amt FLOAT,
    gender CHAR(1),
    zip INT,
    lat FLOAT,
    long FLOAT,
    city_pop INT,
    merch_lat FLOAT,
    merch_long FLOAT,
    "current_time" TIMESTAMP,
    dob DATE,
    age INT,
    hour INT,
    dayofweek INT,
    month INT,
    dayofyear INT,
    dayofmonth INT,
    weekofyear INT,
    distance FLOAT,
    card_issuer_Bank INT,
    card_issuer_MMI VARCHAR(10),
    predicted_fraud BOOLEAN
);
"""
# Définition du DAG
default_args = {
    'owner': 'rl',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'fraud_detection_pipelineopti1-vfinal',
    default_args=default_args,
    description='Pipeline de détection de fraude',
    schedule_interval='@daily',
)

get_transaction_task = PythonOperator(
    task_id='get_transaction_task',
    python_callable=get_transaction,
    provide_context=True,
    dag=dag,
)

analyze_transaction_task = BranchPythonOperator(
    task_id='analyze_transaction_task',
    python_callable=analyze_and_branch,
    provide_context=True,
    dag=dag,
)

handle_fraud_task = PythonOperator(
    task_id='handle_fraud_task',
    python_callable=handle_fraud,
    provide_context=True,
    dag=dag,
)

send_fraud_email_task = PythonOperator(
    task_id='send_fraud_email_task',
    python_callable=send_fraud_email,
    provide_context=True,
    dag=dag,
)

send_no_fraud_email_task = PythonOperator(
    task_id='send_no_fraud_email_task',
    python_callable=send_no_fraud_email,
    provide_context=True,
    dag=dag,
)

no_fraud_task = PythonOperator(
    task_id='no_fraud_task',
    python_callable=no_fraud,
    provide_context=True,
    dag=dag,
)

save_to_s3_task = PythonOperator(
    task_id='save_to_s3_task',
    python_callable=save_to_s3,
    provide_context=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

# Tâche pour créer la table dans Redshift
create_redshift_table = RedshiftSQLOperator(
    task_id='create_redshift_table',
    sql=create_table_sql,
    redshift_conn_id='redshift_default',
    dag=dag
)


# Tâche pour créer la table dans Redshift
create_redshift_table = PostgresOperator(
    task_id='create_redshift_table',
    sql=create_table_sql,
    postgres_conn_id='redshift_default',
    dag=dag
)

s3_to_redshift = S3ToRedshiftOperator(
    task_id='s3_to_redshift',
    schema='public',
    table='fraud_transactions',
    s3_bucket='{{ var.value.S3BucketName }}',
    s3_key='{{ ti.xcom_pull(task_ids="save_to_s3_task", key="s3_file_path") }}',
    copy_options=[
        "json 'auto'",
        "maxerror 10",
        "truncatecolumns",
        "emptyasnull",
        "blanksasnull",
        "timeformat 'auto'"
    ],
    redshift_conn_id='redshift_default',
    aws_conn_id='aws_default',
    dag=dag
)

# Modifiez l'ordre des tâches
get_transaction_task >> analyze_transaction_task
analyze_transaction_task >> handle_fraud_task >> send_fraud_email_task >> save_to_s3_task
analyze_transaction_task >> no_fraud_task >> send_no_fraud_email_task >> save_to_s3_task
save_to_s3_task >> create_redshift_table >> s3_to_redshift