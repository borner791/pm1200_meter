import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from meter_formatter import influx_formatter
import time
import threading
import queue



class influxer(threading.Thread):
    def __init__(self, bucket, org, token, url, measurement, tags,batch_size=12):
        self.bucket = bucket
        self.inflx_org = org
        self.inflx_token = token
        self.inflx_server = url
        self.tags = tags
        self.measurement = measurement
        self.client = influxdb_client.InfluxDBClient(url=self.inflx_server,
                                                     token=self.inflx_token,
                                                     org=self.inflx_org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

        self.BATCH_SIZE = batch_size
        self.dataIn = queue.Queue(2* batch_size)
        self.npoints = 0
        self.batch = []

        self.runThread = True
        threading.Thread.__init__(self)
        self.start()

    def publish_data(self, payload):
        formatted = influx_formatter(self.measurement,self.tags,payload)
        self.dataIn.put_nowait(formatted.get_points())
        # self.write_api.write(bucket=self.bucket,org=self.inflx_org,record=payload)


    def quit(self):
        self.runThread = False
        self.npoints = 0
        self.dataIn.put_nowait(1) #make sure we're not waiting on an empty queue
        self.write_api.flush()
        self.write_api.close()
        self.client.close()
    
    def run(self):
        while self.runThread:
            self.batch.append(self.dataIn.get(block=True)) #Blocks until there is new data
            self.npoints += 1 # do it this way, so even on timeouts we count up and will try and send data if everything else is fucked.
            print(f'influx points {self.npoints}')
            if self.npoints >= self.BATCH_SIZE:
                self.npoints = 0
                self.write_api.write(bucket=self.bucket,org=self.inflx_org,record=self.batch)
                self.batch = []

        



test_token = "zBdePt8IDARdhDkmXG-Y7UkU5GZsD3J2pIkl3q5rfMbUr_mdED7zi2Qxm3Cjak9rLTirdbuRx-CWywUANNUoUg=="
url="http://visual.local:8086"
bucket = "python_test"
org = "bret tech"

if __name__ == '__main__':
    inflx= influxer(bucket,org,test_token,url,'mtetr',{'mID':'bret1'},2)
    data = []
    inflx.publish_data({'B': {'VLN': 119.8, 'hz': 60.01, 'A': 5.32, 'VA': 101.0}, 'A': {'VLN': 120.1, 'hz': 59.99, 'A': 5.32, 'VA': 100.0}})
    time.sleep(5)
    inflx.publish_data({'B': {'VLN': 119.8, 'hz': 60.01, 'A': 5.32, 'VA': 101.0}, 'A': {'VLN': 120.1, 'hz': 59.99, 'A': 6.32, 'VA': 100.0}})
    time.sleep(10)
    inflx.quit()
    inflx.join()
    
    # inflx.publish_data(infxpt.get_points())


