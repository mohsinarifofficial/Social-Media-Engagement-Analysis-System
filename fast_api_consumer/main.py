from fastapi import FastAPI, BackgroundTasks
import asyncio
from aiokafka import AIOKafkaConsumer
import json
from contextlib import asynccontextmanager
import joblib # Import joblib
import numpy as np # Import numpy
from scipy.sparse import csr_matrix, hstack # Import sparse matrix utilities
from sklearn.feature_extraction.text import TfidfVectorizer # Import TF-IDF
from sklearn.preprocessing import OneHotEncoder, LabelEncoder # Import encoders



KAFKA_BROKER_URL = "localhost:9092"
KAFKA_TOPIC = "test"
KAFKA_CONSUMER_ID = "fast-api-consumer"

stop_polling_event = asyncio.Event()

# Variable to hold loaded ML artifacts
ml_artifacts = {}

def json_deserializer(serialized_message):
    # print(f"[Deserializer] Received raw message type: {type(serialized_message)}")
    # print(f"[Deserializer] Received raw message: {serialized_message}")
    if serialized_message is None:
        # print("[Deserializer] Returning None for None input.")
        return None

    # If the message is already a dictionary, return it directly
    if isinstance(serialized_message, dict):
        # print("[Deserializer] Input is already a dictionary, returning directly.")
        return serialized_message
    
    message_to_parse = serialized_message
    if isinstance(serialized_message, bytes):
        try:
            message_to_parse = serialized_message.decode("utf-8")
            # print(f"[Deserializer] Decoded bytes to string.")
        except Exception as e:
            # print(f"[Deserializer] Failed to decode bytes: {e}")
            return None # Return None if decoding fails

    if isinstance(message_to_parse, str):
        try:
            deserialized = json.loads(message_to_parse)
            # print(f"[Deserializer] Deserialized object: {deserialized}")
            return deserialized
        except Exception as e:
            # print(f"[Deserializer] Failed to parse message string: {e}")
            return None # Return None if JSON parsing fails
    else:
        # print(f"[Deserializer] Unexpected message type after potential decoding: {type(message_to_parse)}")
        return None # Return None for unexpected types


async def create_kafkaconsumer():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        auto_offset_reset="earliest",
        value_deserializer=json_deserializer,
        group_id=KAFKA_CONSUMER_ID,
    )
    return consumer

async def poll_consumer(consumer: AIOKafkaConsumer):
    await consumer.start()
    try:
        while not stop_polling_event.is_set():
            # getmany returns {TopicPartition: [messages]} or {} if timeout
            result = await consumer.getmany(timeout_ms=1000)
            for tp, messages in result.items():
                for message in messages:
                    # print(f"[Poll Consumer Debug] message.value type: {type(message.value)}")
                    # print(f"[Poll Consumer Debug] message.value: {message.value}")
                    # Explicitly deserialize the message value
                    deserialized_value = json_deserializer(message.value)
                    # print(f"[Poll Consumer Debug] deserialized_value type: {type(deserialized_value)}")
                    # print(f"[Poll Consumer Debug] deserialized_value: {deserialized_value}")

                    try:
                         if deserialized_value is not None and ml_artifacts:
                             # Extract fields from the deserialized dictionary
                             post_content_text = deserialized_value.get("post_content_text", "") # Provide default empty string
                             social_media_platform = deserialized_value.get("social_media_platform", "Unknown") # Provide default
                             language = deserialized_value.get("language", "Unknown") # Provide default
                             producer_id = deserialized_value.get("producer_id", "Unknown") # Provide default
                             no_of_hashtags = deserialized_value.get("no_of_hashtags", 0) # Provide default 0
                             no_of_mentions = deserialized_value.get("no_of_mentions", 0) # Provide default 0

                             # Perform Feature Engineering matching training
                             # Ensure text fields are strings for len() and split()
                             text_content = str(post_content_text)
                             text_len = len(text_content)
                             word_count = len(text_content.split())

                             # Preprocess features using loaded artifacts
                             tfidf_vector = ml_artifacts['tfidf'].transform([text_content]) # TF-IDF expects iterable
                             ohe_vector = ml_artifacts['onehot'].transform([[social_media_platform, language]]) # OHE expects 2D array

                             # Combine features (numerical features need to be in a sparse matrix for hstack)
                             # Ensure the order matches training: [X_text, X_cat, X_dense]
                             # X_dense order: ['hashtag_count', 'mention_count', 'text_len', 'word_count']
                             numerical_features = csr_matrix(np.array([[no_of_hashtags, no_of_mentions, text_len, word_count]]))

                             # Stack features horizontally
                             X_predict = hstack([tfidf_vector, ohe_vector, numerical_features])

                             # Make prediction using the loaded model (e.g., Random Forest)
                             # You can choose other models from ml_artifacts if preferred
                             model = ml_artifacts.get('Random Forest') # Use .get for safer access

                             if model:
                                 # Predict the engagement rate
                                 # If using XGBoost, predict encoded and then inverse_transform
                                 if 'XGBoost' in ml_artifacts and model == ml_artifacts['XGBoost']:
                                     y_pred_encoded = model.predict(X_predict)
                                     predicted_engagement = ml_artifacts['label_encoder'].inverse_transform(y_pred_encoded)[0]
                                 else:
                                     y_pred = model.predict(X_predict)
                                     predicted_engagement = y_pred[0]

                                 print(f"Processed message with Producer ID: {producer_id}")
                                 print(f"Predicted Engagement Rate: {predicted_engagement}")
                                 print("---") # Separator for clarity
                             else:
                                  print(f"ML model not loaded or found in artifacts for message: {deserialized_value}")
                                  print("---")
                         else:
                             # Handle messages that failed deserialization or if artifacts are not loaded
                             if ml_artifacts:
                                 print(f"Received message with None deserialized value: {message}")
                                 print("---")
                             else:
                                  print(f"Received message but ML artifacts are not loaded: {message}")
                                  print("---")
                    except Exception as e:
                         print(f"Error processing message {message}: {e}")
                         print("---")

            # Small sleep to prevent tight loop when no messages
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
         print("Consumer polling task cancelled.")
    except Exception as e:
        print(f"Error polling consumer: {e}")
    finally:
        await consumer.stop()
        print("Consumer stopped.")

task_list=[]

@asynccontextmanager
async def lifespan_event(app: FastAPI):
    # Startup logic: Load ML artifacts and start consumer task
    print("Starting up application...")
    global ml_artifacts
    try:
        # !!! IMPORTANT: Replace with the actual path to your .pkl file !!!
        artifact_path = "engagement_model_bundle.pkl"
        print(f"Loading ML artifacts from {artifact_path}...")
        ml_artifacts = joblib.load(artifact_path)
        print("ML artifacts loaded successfully.")

        # Start the consumer polling task in the background
        stop_polling_event.clear()
        consumer = await create_kafkaconsumer()
        task_list.append(asyncio.create_task(poll_consumer(consumer)))
        print("Consumer task started successfully.")

    except FileNotFoundError:
        print(f"Error: ML artifact file not found at {artifact_path}")
        # Depending on requirements, you might want to raise the exception
        # or disable the /trigger endpoint if artifacts are essential.
        ml_artifacts = {} # Ensure ml_artifacts is empty if loading fails
    except Exception as e:
        print(f"Error during startup: {e}")
        ml_artifacts = {} # Ensure ml_artifacts is empty if loading fails

    yield # Application is now running

    # Shutdown logic: Signal consumer to stop and wait for it
    print("Shutting down application...")
    stop_polling_event.set()
    if task_list:
        # Assuming only one consumer task is added to task_list
        consumer_task = task_list.pop(0) # Use pop(0) or similar to get the task
        try:
            await asyncio.wait_for(consumer_task, timeout=5.0) # Wait for task to finish with timeout
            print("Consumer task finished.")
        except asyncio.TimeoutError:
            print("Consumer task did not finish within timeout, cancelling...")
            consumer_task.cancel()
            try:
                 await consumer_task # Wait for cancellation to complete
            except asyncio.CancelledError:
                 print("Consumer task cancelled successfully.")
        except Exception as e:
             print(f"Error waiting for consumer task to finish: {e}")
    print("Application shutdown complete.")

# Update FastAPI app initialization to use lifespan
app = FastAPI(lifespan=lifespan_event)

@app.get("/trigger")
async def trigger_polling(): # Removed background_tasks as polling is now async and managed by lifespan
    # The consumer polling task is now started during the lifespan startup event.
    # This endpoint could potentially be used to trigger message processing logic
    # if the consumer wasn't polling continuously, but with continuous polling
    # it might be redundant or used for re-triggering if it stopped.
    # Given the continuous polling in poll_consumer, this endpoint might just
    # serve as a check or a way to see the status, or could be removed.
    if task_list and not task_list[0].done():
         return {"message": "Consumer polling task is running.", "task_done": False}
    elif ml_artifacts: # Check if artifacts are loaded before attempting to restart
        # If the task is done (e.g., stopped due to error or cancellation), try restarting it.
        print("Attempting to restart consumer polling task...")
        stop_polling_event.clear()
        try:
            consumer = await create_kafkaconsumer()
            task_list.append(asyncio.create_task(poll_consumer(consumer)))
            print("Consumer task restarted.")
            return {"message": "Consumer polling task restarted successfully.", "task_done": False}
        except Exception as e:
            print(f"Failed to restart consumer task: {e}")
            return {"message": f"Failed to restart consumer polling task: {e}", "task_done": True} # Indicate failure
    else:
        return {"message": "ML artifacts not loaded, cannot start consumer.", "task_done": True}

@app.get("/stop")
async def stop_trigger():
    # This endpoint is now mainly for signaling the consumer task to stop.
    print("Received stop signal.")
    stop_polling_event.set()
    # The waiting for the task to finish is handled in the lifespan shutdown event.
    if task_list:
        return {"message": "Consumer stop signal sent.", "task_done": task_list[0].done()}
    else:
        return {"message": "No consumer task is running.", "task_done": True}

# The old on_event handler is replaced by the lifespan context manager
# @app.on_event("shutdown")
# async def shutdown_event():
#     print("Shutting down application, stopping consumer...")
#     stop_polling_event.set()
#     if task_list:
#         task = task_list.pop()
#         await task # Await the consumer task to finish stopping
#         print("Consumer task finished.")




