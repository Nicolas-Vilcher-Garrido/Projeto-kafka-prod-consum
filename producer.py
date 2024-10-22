Producer: O código producer envia mensagens para um tópico Kafka para comunicar eventos, registrar logs ou enviar dados de aplicações


from kafka import KafkaProducer
import json
import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=json_serializer,
)

if __name__ == '__main__':
    while True:
        sample_data = {"name": "user", "age": 30}
        producer.send("teste-topic", sample_data)
        print(f"Sent: {sample_data}")
        time.sleep(2)
