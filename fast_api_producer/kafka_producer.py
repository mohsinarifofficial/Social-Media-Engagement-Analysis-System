from kafka import KafkaProducer
from produce_schema import ProduceMessage
from fastapi import HTTPException
import json

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "test"
PRODUCER_CLIENT_ID="fastapi-producer"

def serializer(message):
    return json.dumps(message).encode("utf-8")

producer = KafkaProducer(
    api_version=(0,8,0),
    bootstrap_servers=KAFKA_BROKER,
    client_id=PRODUCER_CLIENT_ID,
    value_serializer=serializer
)

def produce_kafka_message(messageRequest: ProduceMessage):
    try:
        # Send the dictionary representation of the ProduceMessage object
        producer.send(KAFKA_TOPIC, messageRequest.model_dump())
        producer.flush()
        return {"message": "Message sent successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))







