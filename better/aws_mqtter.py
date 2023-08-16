from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import json
import time

class aws_ingestor:

    def __init__(self,endpoint, topic, clientID, certPath, privateKeyPath, CAPath, port=443, qos = mqtt.QoS.AT_LEAST_ONCE):
        self.EP = endpoint
        self.topic = topic
        self.CID = clientID
        self.port = port
        self.connected = False
        self.DeviceCert = certPath
        self.DeviceKey = privateKeyPath
        self.AWS_CA = CAPath
        self.qos = qos

        self.mqtt_connection = mqtt_connection_builder.mtls_from_path(
                            endpoint=self.EP,
                            port=self.port,
                            cert_filepath=self.DeviceCert,
                            pri_key_filepath=self.DeviceKey,
                            ca_filepath=self.AWS_CA,
                            on_connection_interrupted=self.on_connection_interrupted,
                            on_connection_resumed=self.on_connection_resumed,
                            client_id=self.CID,
                            clean_session=False,
                            keep_alive_secs=90,
                            on_connection_success=self.on_connection_success,
                            on_connection_failure=self.on_connection_failure,
                            on_connection_closed=self.on_connection_closed)
    
        self.connect_future = self.mqtt_connection.connect()
        self.connect_future.result()
        print("Connected!")


        # Callback when connection is accidentally lost.


    def on_connection_interrupted(self,connection, error, **kwargs):
        print("Connection interrupted. error: {}".format(error))
        self.connected = False


    # Callback when an interrupted connection is re-established.
    def on_connection_resumed(self,connection, return_code, session_present, **kwargs):
        print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))
        self.connected = True

    def on_resubscribe_complete(self,resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

    # Callback when the subscribed topic receives a message
    def on_message_received(self,topic, payload, dup, qos, retain, **kwargs):
        print("Received message from topic '{}': {}".format(topic, payload))

    # Callback when the connection successfully connects
    def on_connection_success(self,connection, callback_data):
        assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
        print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))
        self.connected = True

    # Callback when a connection attempt fails
    def on_connection_failure(self,connection, callback_data):
        assert isinstance(callback_data, mqtt.OnConnectionFailuredata)
        print("Connection failed with error code: {}".format(callback_data.error))
        self.connected = False

    # Callback when a connection has been disconnected or shutdown successfully
    def on_connection_closed(self,connection, callback_data):
        print("Connection closed")
        self.connected = False
    
    def publish_data(self, payload):
        if self.connected:
            self.mqtt_connection.publish(
                    topic=self.topic,
                    payload=payload,
                    qos=self.qos)
            return True
        else:
            return False




END_POINT = "a18080ddzg98lm-ats.iot.us-east-2.amazonaws.com"
CLIENT_ID = "pm_test1"

TOPIC = "pm/reading"

Dev_cert = '/home/bret/dev/powermeter/aws_iot/1f586ed555d4792b75832c56dcb793a3cc5d50d873574ec87d56ffbd11987817-certificate.pem.crt'

Dev_Key = '/home/bret/dev/powermeter/aws_iot/1f586ed555d4792b75832c56dcb793a3cc5d50d873574ec87d56ffbd11987817-private.pem.key'

AWS_CA = '/home/bret/dev/powermeter/aws_iot/AmazonRootCA1.pem'

if __name__ == '__main__':

    ingestor = aws_ingestor(endpoint=END_POINT,topic=TOPIC,clientID=CLIENT_ID,certPath=Dev_cert,privateKeyPath=Dev_Key,CAPath=AWS_CA)

    
    message = dict()
    message['data'] = []

    for i in range(5):
        message['data'].append({'mID':f'bret{i}',"V_phA":120.0+(10*i),"V_phB":121.1+(20*i),"A_phA":12.1+(30*i),"A_phB":9.999+i} )
    print("Publishing message to topic '{}': {}".format(TOPIC, message))

    message_json = json.dumps(message)
    while not ingestor.publish_data(message_json):
        print("waiting for connect")
        time.sleep(1)
    
    
    time.sleep(2)


    