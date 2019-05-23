import json

from flask import Flask, request, abort
from kafka import KafkaProducer


# Default Flask configuration
app = Flask(__name__)

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))


@app.route('/', methods=['POST'])
def index():
    app.logger.info('Event: ' + str(request.get_json()))
    process_event(request.get_json())
    return ''


def permitted(id, token):
    """
    Each dispenser has a hardcoded token. We store mappings from
    the dispenser id to the tokens, and check that these match on each
    request.

    While this does NOT provide a secure way of authenticating dispensers,
    it is a lightweight approach that allows us some level of protection against
    bots and the like.
    """

    # TODO Store these mappings in a database.
    permitted_devices = {1: '42x5yz'}

    return permitted_devices.get(id) == token


def process_event(event):
    """
    Events are first authenticated against a database of id to token mappings.
    If this succeeds, events are dispatched to an appropriate handler, determined
    by the type field.
    """

    # Extract the required parameters from the request body
    id = event.get('id')
    token = event.get('token')
    type = event.get('type')

    # If any of the required parameters are absent, reject the request.
    if (not id) or (not token) or (not type):
        abort(400)

    # Authenticate the dispenser against our database of tokens.
    if not permitted(event['id'], event['token']):
        abort(401)

    # Send the event to the dispensers Kafka topic
    # TODO Topic for startup, dispense, battery level, refill request
    producer.send('dispensers', event)


if __name__ == '__main__':
    app.run()
