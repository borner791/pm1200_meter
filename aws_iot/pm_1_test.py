from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import json
import time

END_POINT = "a18080ddzg98lm-ats.iot.us-east-2.amazonaws.com"
CLIENT_ID = "pm_test1"

TOPIC = "pm/reading"
# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))

# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailuredata)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")

if __name__ == '__main__':


    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=END_POINT,
        port=443,
        cert_filepath="1f586ed555d4792b75832c56dcb793a3cc5d50d873574ec87d56ffbd11987817-certificate.pem.crt",
        pri_key_filepath="1f586ed555d4792b75832c56dcb793a3cc5d50d873574ec87d56ffbd11987817-private.pem.key",
        ca_filepath="AmazonRootCA1.pem",
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=CLIENT_ID,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed)
    
    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    message = "{} [{}]".format("testMeter", 1)
    message = {'mID':"bret","V_phA":120.0,"V_phB":121.1,"A_phA":12.1,"A_phB":9.999}
    print("Publishing message to topic '{}': {}".format(TOPIC, message))
    

    message_json = json.dumps(message)
    mqtt_connection.publish(
                topic=TOPIC,
                payload=message_json,
                qos=mqtt.QoS.AT_LEAST_ONCE)
    
    time.sleep(20)


    