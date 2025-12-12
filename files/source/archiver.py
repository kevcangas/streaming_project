import json
import polars as pl
import boto3
import os
import io

from datetime import date
from kafka import KafkaConsumer

# Configuración obtenida de variables de entorno (Docker)
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_ENDPOINT = os.getenv('S3_ENDPOINT_URL')
BUCKET_NAME = "transactions"
BATCH_SIZE = 10

def get_s3_client():
    """Inicializa el cliente boto3 para MinIO"""
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT
    )

def load_data(df:pl.DataFrame, date, batch_num: int) -> None:
    """LOAD: Guardar en MinIO como Parquet"""
    print("--- Iniciando Carga ---")
    s3 = get_s3_client()
    
    # Crear bucket si no existe
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' asegurado.")
    except Exception as e:
        print(f"Nota sobre bucket: {e}")

    # Guardar dataframe como Parquet en memoria
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    
    # Subir a S3/MinIO
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    file_name = rf"coldpath/{year}/{month}/{day}/data-batch-{batch_num}.parquet"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=buffer.getvalue()
    )
    print(f"Archivo guardado exitosamente en s3://{BUCKET_NAME}/{file_name}")

if __name__ == '__main__':

    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='latest',
        group_id='storage-group', # Importante para Kafka
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Escuchando transacciones sospechosas...")

    data_batch = []
    current_date = date.today()
    current_batch = 0
    
    for msg in consumer:
        data = msg.value
        data_batch.append(data)

        if len(data_batch) >= BATCH_SIZE:
            df = pl.DataFrame(data_batch)
            load_data(df, current_date, current_batch)
            current_batch = current_batch + 1
            print(f"Loaded batch {current_batch}")

            data_batch = []


