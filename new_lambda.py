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
