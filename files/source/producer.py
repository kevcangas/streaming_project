import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# Conexión al broker Kafka
# Nota: 'kafka:29092' es la dirección interna en la red docker
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=json_serializer
)

if __name__ == "__main__":
    print("Iniciando simulador de transacciones...")
    while True:
        # Generar transacción falsa
        transaction = {
            "id": fake.uuid4(),
            "user_id": fake.random_int(min=1, max=1000),
            "amount": round(random.uniform(10.0, 10000.0), 2), # De $10 a $10,000
            "currency": "USD",
            "timestamp": time.time(),
            "merchant": fake.company()
        }
        
        # Enviar al tópico 'transactions'
        producer.send("transactions", transaction)
        print(f"Enviado: {transaction['amount']} USD")
        
        # Simular tráfico (1 transacción por segundo)
        time.sleep(1)