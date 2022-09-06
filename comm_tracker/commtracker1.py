from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

project_id = "bradybyrd-POC"
subscription_id = "comm-msg-topic-sub"
timeout = 60.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

message_count = 0
bulk_docs = []
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    if message_count == 10:
        db = client_connection()
        db = self.conn[self.settings["database"]]
        self.logit(f"Loading batch: {self.batch_size} - total: {self.counter}")
        ans = db[self.settings["collection"]].insert_many(self.bulk_docs)
        self.bulk_docs = []
        self.logit(f'Bulk Docs: ')
        pprint.pprint(self.bulk_docs)

    message_count += 1
    bulk_docs.append(doc)

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

