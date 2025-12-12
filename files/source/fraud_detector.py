import json
from kafka import KafkaConsumer

if __name__ == '__main__':

    consumer = KafkaConsumer(
        "transactions",
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='latest',
        group_id='fraud-group', # Importante para Kafka
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Escuchando transacciones sospechosas...")

    for msg in consumer:
        data = msg.value
        # REGLA DE NEGOCIO: Si es mayor a $5,000 es sospechoso
        if data['amount'] > 5000:
            print(f"ALERTA DE FRAUDE: Transacción de ${data['amount']} en {data['merchant']} (User: {data['user_id']})")