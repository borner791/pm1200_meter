from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import json
import time
import threading
import queue
from meter_formatter import aws_formatter
from systemd import journal

class aws_ingestor(threading.Thread):

    def __init__(self,endpoint, topic, clientID, certPath, privateKeyPath, CAPath, port=443, qos = mqtt.QoS.AT_LEAST_ONCE, batch_size = 12):
        self.EP = endpoint
        self.topic = topic
        self.CID = clientID
        self.port = port
        self.connected = False
        self.reconnectCtr = 0
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
                            on_connection_interrupted=self._on_connection_interrupted,
                            on_connection_resumed=self._on_connection_resumed,
                            client_id=self.CID,
                            clean_session=False,
                            keep_alive_secs=90,
                            on_connection_success=self._on_connection_success,
                            on_connection_failure=self._on_connection_failure,
                            on_connection_closed=self._on_connection_closed)
    
        self.connect_future = self.mqtt_connection.connect()
        self.connect_future.result()
        journal.write("Connected!")

        self.BATCH_SIZE = batch_size
        self.dataIn = queue.Queue(2* batch_size)
        self.npoints = 0
        self.batch = []

        self.runThread = True
        threading.Thread.__init__(self)
        self.start()

    # Callback when connection is accidentally lost.
    def _on_connection_interrupted(self,connection, error, **kwargs):
        journal.write("Connection interrupted. error: {}".format(error))
        self.connected = False


    # Callback when an interrupted connection is re-established.
    def _on_connection_resumed(self,connection, return_code, session_present, **kwargs):
        journal.write("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))
        self.connected = True

    def _on_resubscribe_complete(self,resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        journal.write("Resubscribe results: {}".format(resubscribe_results))

    # Callback when the subscribed topic receives a message
    def _on_message_received(self,topic, payload, dup, qos, retain, **kwargs):
        journal.write("Received message from topic '{}': {}".format(topic, payload))

    # Callback when the connection successfully connects
    def _on_connection_success(self,connection, callback_data):
        assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
        journal.write("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))
        self.connected = True
        self.reconnectCtr += 1 

    # Callback when a connection attempt fails
    def _on_connection_failure(self,connection, callback_data):
        assert isinstance(callback_data, mqtt.OnConnectionFailuredata)
        journal.write("Connection failed with error code: {}".format(callback_data.error))
        self.connected = False

    # Callback when a connection has been disconnected or shutdown successfully
    def _on_connection_closed(self,connection, callback_data):
        journal.write("Connection closed")
        self.connected = False
    
    def publish_data(self, payload):
        formatted = aws_formatter(self.CID,payload)
        if not self.dataIn.full():
            self.dataIn.put_nowait(formatted.get_points())
        ###
        
    def quit(self):
        self.runThread = False
        self.npoints = 0
        self.dataIn.put_nowait(1) #make sure we're not waiting on an empty queue
        self.mqtt_connection.disconnect()
    
    def run(self):
        while self.runThread:
            self.batch.append(self.dataIn.get(block=True)) #Blocks until there is new data
            self.npoints += 1 # do it this way, so even on timeouts we count up and will try and send data if everything else is fucked.
            journal.write(f'aws points {self.npoints} reconnects {self.reconnectCtr}')
            if self.npoints >= self.BATCH_SIZE:
                report = {'data':self.batch}
                # journal.write(report)
                if self.connected:
                    self.mqtt_connection.publish(
                            topic=self.topic,
                            payload=json.dumps(report),
                            qos=self.qos)
                    self.npoints = 0
                    self.batch = []
                



END_POINT = "a18080ddzg98lm-ats.iot.us-east-2.amazonaws.com"
CLIENT_ID = "pm_test1"

TOPIC = "pm/reading"

Dev_cert = '/home/bret/dev/powermeter/aws_iot/1f586ed555d4792b75832c56dcb793a3cc5d50d873574ec87d56ffbd11987817-certificate.pem.crt'

Dev_Key = '/home/bret/dev/powermeter/aws_iot/1f586ed555d4792b75832c56dcb793a3cc5d50d873574ec87d56ffbd11987817-private.pem.key'

AWS_CA = '/home/bret/dev/powermeter/aws_iot/AmazonRootCA1.pem'

if __name__ == '__main__':

    ingestor = aws_ingestor(endpoint=END_POINT,topic=TOPIC,clientID=CLIENT_ID,certPath=Dev_cert,privateKeyPath=Dev_Key,CAPath=AWS_CA,batch_size=5)

    
    message = dict()
    message['data'] = []

    for i in range(5):
        ingestor.publish_data({'B': {'VLN': 120.0+(10*i), 'hz': 60.01, 'A': 9.999+i, 'VA': 101.0}, 'A': {'VLN': 121.1+(20*i), 'hz': 59.99, 'A': 12.1+(30*i), 'VA': 100.0}})        
    journal.write("Publishing message to topic '{}': {}".format(TOPIC, message))

    
    time.sleep(10)
    ingestor.quit()
    ingestor.join()


    