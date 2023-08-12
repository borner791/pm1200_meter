import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import time


bucket = "python_test"
org = "bret tech"
token = "iNXaU7IW7xaUA9b9WlW20hC7s5WmzmYBjRVR1Vs6ibugcSKOY6WSWXxumonRnZcuh14QnFlS9TiTErCW6gGwYQ=="
# Store the URL of your InfluxDB instance
url="http://192.168.1.4:8086"

client = influxdb_client.InfluxDBClient(
   url=url,
   token=token,
   org=org
)

write_api = client.write_api(write_options=SYNCHRONOUS)

i=0 
while i < 10:
    i+=1
    p = []
    p.append(influxdb_client.Point("power_meter_VLN").tag("phase", "A").field("voltage", 119.0))
    p.append(influxdb_client.Point("power_meter_HZ").tag("phase", "A").field("frequency", 59.9))
    write_api.write(bucket=bucket, org=org, record=p)


    # p = influxdb_client.Point("power_meter").tag("phase", "A").field("frequency", 60.1)
    # write_api.write(bucket=bucket, org=org, record=p)
    print(i)
    time.sleep(5)