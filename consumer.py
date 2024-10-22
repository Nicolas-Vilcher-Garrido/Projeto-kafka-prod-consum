O código do consumer lê  mensagens do tópico, permitindo que as aplicações processem ou respondam a eventos em tempo real.


from kafka import KafkaConsumer
import json

def json_deserializer(data):
    return json.loads(data.decode("utf-8"))

consumer = KafkaConsumer(
    "test-topic",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumer-group-a",
    value_serializer=json_deserializer
)

if __name__ == "__main__":
    for message in consumer:
        print(f"Received: {message.value}")
