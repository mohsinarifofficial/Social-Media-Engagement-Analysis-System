from fastapi import FastAPI, BackgroundTasks
from kafka.admin import KafkaAdminClient, NewTopic
from kafka_producer import produce_kafka_message
from contextlib import asynccontextmanager
from produce_schema import ProduceMessage



KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "test"

KAFKA_ADMIN_CLIENT="fastapi-admin-client"


@asynccontextmanager
async def lifespan(app: FastAPI):
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id=KAFKA_ADMIN_CLIENT
    )
    if not KAFKA_TOPIC in admin_client.list_topics():
        admin_client.create_topics(new_topics=[NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)],validate_only=False)
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/produce",tags=["produce message"])
async def produce_data(messageRequest: ProduceMessage, background_tasks: BackgroundTasks):
    background_tasks.add_task(produce_kafka_message, messageRequest)
    return {"message": "Message sent successfully"}


