from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import smtplib
from email.message import EmailMessage

# Fonction qui envoie l'e-mail avec smtplib
def send_email_with_smtp(subject, body, to_email):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587  # Port pour TLS
    smtp_user = "xxx@gmail.com"
    smtp_password = "xxxx"  # Utilisez un mot de passe d'application si 2FA activé

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

# Arguments du DAG
default_args = {
    'owner': 'RL',
    'start_date': datetime(2024, 10, 4),
    'email_on_failure': True,
    'email_on_retry': False,
}

# Définir le DAG
dag = DAG(
    'send_email_dag',
    default_args=default_args,
    schedule_interval=None,  # Pas de planification (exécution manuelle)
)

# Tâche PythonOperator pour appeler la fonction send_email_with_smtp
send_email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email_with_smtp,
    op_args=["Alerte Frauduleuse", "<h3>Bonjour, une alerte de fraude a été détectée.</h3>", "xxxxx@gmail.com"],
    dag=dag,
)
