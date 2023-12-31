import json
import boto3
import datetime

def lambda_handler(event, context):
    
    client = boto3.client('timestream-write')
    
    records = []
    
    times = ''
    
    for readings in event['data']:
        mValues = []
        point = dict()
        for measure in readings['points']:
            mValues.append({"Name": f'{measure}',"Value": f'{readings["points"][measure]}',"Type": "DOUBLE"})
            
        point['Dimensions'] = [{'Name': 'mID', 'Value': '%s' % readings['mID'],'DimensionValueType': 'VARCHAR'}]
        point['MeasureName'] = 'power_meter'
        point['Version'] = 123
        point['MeasureValues'] = mValues
        point['MeasureValueType'] = 'MULTI'
        point['Time'] = str(int(readings['time']))
        point['TimeUnit'] = 'SECONDS'
        records.append(point)
        ts = datetime.datetime.fromtimestamp(int(readings['time']))
        times += f'{ts.strftime("%Y-%m-%d %H:%M:%S")}, '
    try:
        print(f'RXed {len(event["data"])} points:\n{times}')
        client.write_records(DatabaseName='power_meter',TableName='meter_readings',
                             Records = records)
    except client.exceptions.RejectedRecordsException as err:
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
            if "ExistingVersion" in rr:
                print("Rejected record existing version: ", rr["ExistingVersion"])
    except Exception as err:
        print("Error:", err)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
