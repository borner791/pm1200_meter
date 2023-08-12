
event = {
  "data": [
    {
      "points": {
        "A": {
          "VA": 0.0,
        },
        "B": {
          "VA": 0.0,
        },
        "avg": {
          "VA": 0.0,
        },
        "energy_fwd": {
          "VAH": 0.0,
        },
        "energy_rev": {
          "VAH": 0.0,
        },
        "energy_tot": {
          "VAH": 0.0,
        }
      },
      "time": 1691883269.9554524,
      "mID": "bret"
    },
    {
      "points": {
        "A": {
          "VA": 0.0,
        },
        "B": {
          "VA": 0.0,
        },
        "avg": {
          "VA": 0.0,
        },
        "energy_fwd": {
          "VAH": 0.0,
        },
        "energy_rev": {
          "VAH": 0.0,
        },
        "energy_tot": {
          "VAH": 0.0,
        }
      },
      "time": 1691883275.665604,
      "mID": "bret"
    }
  ]
}



# 'MeasureValues': [
#                     {
#                         "Name": "V_phA",
#                         "Value": '%s' % reading['V_phA'],
#                         "Type": "DOUBLE"
#                     },]

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

print(records)
    
