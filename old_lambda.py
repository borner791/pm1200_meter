import json
import boto3
import time

def lambda_handler(event, context):
    client = boto3.client('timestream-write')
    
    for reading in event['data']:
        print(reading)
        client.write_records(DatabaseName='pm_test1',TableName='meter',
                            Records = [{'Dimensions':[{'Name': 'mID', 'Value': '%s' % reading['mID'],'DimensionValueType': 'VARCHAR'}],
                                       'MeasureName': 'power_meter',
                                       'Version': 123,
                                       'MeasureValues': [
                                           {
                                                "Name": "V_phA",
                                                "Value": '%s' % reading['V_phA'],
                                                "Type": "DOUBLE"
                                            },
                                            {
                                                "Name": "V_phB",
                                                "Value": '%s' % reading['V_phB'],
                                                "Type": "DOUBLE"
                                            },
                                            {
                                                "Name": "A_phA",
                                                "Value": '%s' % reading['A_phA'],
                                                "Type": "DOUBLE"
                                            },
                                            {
                                                "Name": "A_phB",
                                                "Value": '%s' % reading['A_phB'],
                                                "Type": "DOUBLE"
                                            }
                                        ],
                                        'MeasureValueType': 'MULTI',
                                        'Time':str(int(time.time() * 1000))
                                    }]
                            )

    return {
        'statusCode': 200,
        'body': "cool"
    }
