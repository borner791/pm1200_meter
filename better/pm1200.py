import meter_reader
import time

import aws_mqtter
import local_influxer

##LOCAL INFLUX CONFIG
influx_bucket = "pm1200_meter"
influx_token = "yrNck4k72lMnsPTi-fWzI011YO2RGSo17nNsuysPbbedUgRQW8Oq-460HdKAX75iyLv46BTb4OW_BUAkXcPHwQ=="
influx_url="http://visual.local:8086"
influx_org = "bret tech"

##AWS Config

IoT_END_POINT = "a18080ddzg98lm-ats.iot.us-east-2.amazonaws.com"
AWS_clientid = "pm_test1"
IoT_topic = "pm/reading"
AWS_Dev_cert = '/home/bret/dev/powermeter/aws_iot/1f586ed555d4792b75832c56dcb793a3cc5d50d873574ec87d56ffbd11987817-certificate.pem.crt'
AWS_Dev_Key = '/home/bret/dev/powermeter/aws_iot/1f586ed555d4792b75832c56dcb793a3cc5d50d873574ec87d56ffbd11987817-private.pem.key'
AWS_CA = '/home/bret/dev/powermeter/aws_iot/AmazonRootCA1.pem'


if __name__ == '__main__':
    pm1200 = meter_reader.meter_reader('/dev/ttyUSB0',1)
    localInflux = local_influxer.influxer(bucket=influx_bucket,org=influx_org,token=influx_token,url=influx_url,measurement="meter",tags={'mID':'bret1'})
    # aws = aws_mqtter.aws_ingestor(IoT_END_POINT,IoT_topic,AWS_clientid,AWS_Dev_cert,AWS_Dev_Key,AWS_CA)


    while True:
        meter_readings=dict()
        start = time.time()
        
        meter_readings |= pm1200.read_rms_block()
        meter_readings |= pm1200.read_quality_blk()
        meter_readings |= pm1200.read_energy_blk()
        print(meter_readings)
        localInflux.publish_data(meter_readings)

        print(f'{time.time()-start} - {pm1200.connects}')
        time.sleep(5)