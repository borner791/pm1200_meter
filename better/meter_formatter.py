import json
from datetime import datetime


# https://influxdb-python.readthedocs.io/en/latest/examples.html

# json_body = [
#         {
#             "measurement": "cpu_load_short",
#             "tags": {
#                 "host": "server01",
#                 "region": "us-west"
#             },
#             "time": "2009-11-10T23:00:00Z",
#             "fields": {
#                 "Float_value": 0.64,
#                 "Int_value": 3,
#                 "String_value": "Text",
#                 "Bool_value": True
#             }
#         }
#     ]
#  client.write_points(json_body)


class influx_formatter:
    def __init__(self, measurement, tags, pmpoints):
        self.jsonout ={"measurement": measurement, "tags":dict(), "time":"","fields":dict()}
        self.jsonout['tags'] = tags
        self.jsonout['time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        for phase in pmpoints:
            for point in pmpoints[phase]:
                self.jsonout['fields'] |= {f"{point}_{phase}":pmpoints[phase][point]}
            
    def get_points(self):
        return self.jsonout
        
        
    
class aws_formatter:
    def __init__(self,meterid, pmpoints):
        self.jsonout = {'points':dict(),'mID':meterid,'time':int(datetime.utcnow().timestamp())}
        for phase in pmpoints:
            for point in pmpoints[phase]:
                self.jsonout['points'] |= {f"{point}_{phase}":pmpoints[phase][point]}
    def get_points(self):
        return self.jsonout
    

if __name__ == '__main__':
    pha = {'VLN':120.1, 'hz':59.99, 'A':4.32,'VA':100.0}
    phb = {'VLN':119.8, 'hz':60.01, 'A':5.32,'VA':101.0}
    meter_input  = {'B':phb,'A':pha}

    print (meter_input)

    fields = dict()
    for phase in meter_input:
        for point in meter_input[phase]:
            fields |= {f"{point}_{phase}":meter_input[phase][point]}

    print(fields)

    print(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))

    forInflux = influx_formatter("powermeter",{'mid':'bret'},meter_input)
    print(forInflux.get_points())

    foraws = aws_formatter("bret1",meter_input)
    print(foraws.get_points())

