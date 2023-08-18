# {
#   "data": [
#     {
#       "points": {
#         "A": {"VLN": 120.82274627685547, "A": 2.0000001329864406e-17, "HZ": 60.0186882019043},
#         "B": { "VLN": 1.000000045813705e-18, "A": 2.0000001329864406e-17, "HZ": 60.022621154785156},
#         "avg": { "VLN": 40.27425003051758,"A": 2.0000001329864406e-17, "HZ": 60.0186882019043},
#         "energy_fwd": {"WH": 0},
#         "energy_rev": {"WH": 0},
#         "energy_tot": {"WH": 0}
#       },
#       "time": 1691883269.9554524,
#       "mID": "bret"
#     },
#     {
#       "points": {
#         "A": {"VLN": 120.82274627685547, "A": 2.0000001329864406e-17, "HZ": 60.0186882019043},
#         "B": { "VLN": 1.000000045813705e-18, "A": 2.0000001329864406e-17, "HZ": 60.022621154785156},
#         "avg": { "VLN": 40.27425003051758,"A": 2.0000001329864406e-17, "HZ": 60.0186882019043},
#         "energy_fwd": {"WH": 0},
#         "energy_rev": {"WH": 0},
#         "energy_tot": {"WH": 0}
#       },
#       "time": 1691883269.9554524,
#       "mID": "bret"
#     },
#   ]
#}


import json
import boto3

def lambda_handler(event, context):
    
    client = boto3.client('timestream-write')
    
    records = []
    for readings in event['data']:
        mValues = []
        point = dict()
        for phase in readings['points']:
            for measure in readings['points'][phase]:
                mValues.append({"Name": f'{measure}_{phase}',"Value": f'{readings["points"][phase][measure]}',"Type": "DOUBLE"})
        point['Dimensions'] = [{'Name': 'mID', 'Value': '%s' % readings['mID'],'DimensionValueType': 'VARCHAR'}]
        point['MeasureName'] = 'power_meter'
        point['Version'] = 123
        point['MeasureValues'] = mValues
        point['MeasureValueType'] = 'MULTI'
        point['Time'] = str(int(readings['time']))
        point['TimeUnit'] = 'SECONDS'
        records.append(point)
    
    client.write_records(DatabaseName='pm_test1',TableName='meter',
                         Records = records
                            )
    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
