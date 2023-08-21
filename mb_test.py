import minimalmodbus
import time
import struct
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import json

bucket = "power_meter"
org = "bret tech"
token = "0uIVgTs4RnGOQuLe1GPC6-e-WUN3q77OdlmAjf68vIOCBPa7BVqqckssMB0is9-D3KYSbRtvcNGXmoEfpq-J6w=="
# Store the URL of your InfluxDB instance
url="http://visual.local:8086"



def registers_2_floats(resp, nregs,keys):
    
    n_regs = 2
    vals = {}
    idx = 0

    for key in keys:
        cur_idx = n_regs*idx
        idx+=1
        packed_bytes = struct.pack("HH",resp[cur_idx],resp[cur_idx+1])
        formatcode = f'<{keys[key]}'
        value = float(struct.unpack(formatcode, packed_bytes)[0])
        vals[key] = value
    return vals



dbclient = influxdb_client.InfluxDBClient(
   url=url,
   token=token,
   org=org
)
dbwrite = dbclient.write_api(write_options=SYNCHRONOUS)


instr = minimalmodbus.Instrument('/dev/ttyUSB0',1,minimalmodbus.MODE_RTU,False,True)
instr.serial.baudrate = 19200
instr.serial.parity = minimalmodbus.serial.PARITY_EVEN
instr.serial.stopbits = minimalmodbus.serial.STOPBITS_ONE
instr.serial.timeout = 0.5


# voltage   = {'VLL':3908, 'VLN':3910,'V12':3924, 'V1LN':3926,'V2LN':3940}
# current = {'A':3912,'A1':3928,'A2':3942}
# power = {'W':3902,'W1':3918,'W2':3932,'VAR':3904,'VAR1':3920,'VAR2':3934,'VA':3900,'VA1':3916,'VA2':3930,'PF':3906,'PF1':3922,'PF2':3936,'F':3914,'V1THD':3860,'V2THD':3862,'A1THD':3866,'A2THD':3868 }

# start = time.time()
# for reg in voltage:
#     volt = instr.read_float(voltage[reg],byteorder=minimalmodbus.BYTEORDER_LITTLE_SWAP)
#     print(f'{reg} : {volt}')
# for reg in current:
#     volt = instr.read_float(current[reg],byteorder=minimalmodbus.BYTEORDER_LITTLE_SWAP)
#     print(f'{reg} : {volt}')
# for reg in power:
#     volt = instr.read_float(power[reg],byteorder=minimalmodbus.BYTEORDER_LITTLE_SWAP)
#     print(f'{reg} : {volt}')

# print(time.time()-start)

reports = []
reportCnt = 0


while True:
    try:
        jsonout = dict()
        jsonout['points'] = {'A':dict(),'B':dict(),'avg':dict(),'energy_fwd':dict(),'energy_rev':dict(),'energy_tot':dict()}


        start = time.time()
        nregs = 20
        RMS_BLK = {'VA':'f','W':'f', 'VAR':'f','PF':'f','VLL':'f','VLN':'f','A':'f','HZ':'f','res':'l','res':'L'}
        statr_reg = 3000
        regs = instr.read_registers(statr_reg,nregs)
        avg_rms = registers_2_floats(regs,nregs/2, RMS_BLK)

        statr_reg = 3030
        regs = instr.read_registers(statr_reg,nregs)
        phA_rms = registers_2_floats(regs,nregs/2, RMS_BLK)

        statr_reg = 3060
        regs = instr.read_registers(statr_reg,nregs)
        phB_rms = registers_2_floats(regs,nregs/2, RMS_BLK)
        p = []
        
        for field in RMS_BLK:
            p.append(influxdb_client.Point(field).tag("phase", "AVG").field('avg', avg_rms[field]))
            p.append(influxdb_client.Point(field).tag("phase", "A").field('ph_a', phA_rms[field]))
            p.append(influxdb_client.Point(field).tag("phase", "B").field('ph_b', phB_rms[field]))
            jsonout['points']['A'][field]=phA_rms[field]
            jsonout['points']['B'][field]=phB_rms[field]
            jsonout['points']['avg'][field]=avg_rms[field]
        

        POWER_QUALITY_BLK = {'V1THD':'f','V2THD':'f','V3THD':'f','A1THD':'f','A2THD':'f'}
        statr_reg = 3860
        nregs = 10
        regs = instr.read_registers(statr_reg,nregs)
        p_qual = registers_2_floats(regs,nregs/2, POWER_QUALITY_BLK)
        
        p.append(influxdb_client.Point("VTHD").tag("phase", "A").field('V1THD', p_qual['V1THD']))
        p.append(influxdb_client.Point("VTHD").tag("phase", "B").field('V2THD', p_qual['V2THD']))
        p.append(influxdb_client.Point("ATHD").tag("phase", "A").field('A1THD', p_qual['A1THD']))
        p.append(influxdb_client.Point("ATHD").tag("phase", "B").field('A2THD', p_qual['A2THD']))
        jsonout['points']['A']['V1THD']=p_qual['V1THD']
        jsonout['points']['B']['V2THD']=p_qual['V2THD']
        jsonout['points']['A']['A1THD']=p_qual['A1THD']
        jsonout['points']['B']['A2THD']=p_qual['A2THD']

                    


        ENERGY_BLK = {'VAH':'f','WH':'f', 'VARHi':'f','res':'l','res':'l','VARHc':'f','res':'l','res':'l','res':'l','Sec':'L'}
        statr_reg = 3121
        nregs = 20
        regs = instr.read_registers(statr_reg,nregs)
        fwd_int = registers_2_floats(regs,nregs/2, ENERGY_BLK)
        # print(fwd_int)

        statr_reg = 3151
        regs = instr.read_registers(statr_reg,nregs)
        rev_int = registers_2_floats(regs,nregs/2, ENERGY_BLK)
        # print(rev_int)

        statr_reg = 3181
        regs = instr.read_registers(statr_reg,nregs)
        tot_int = registers_2_floats(regs,nregs/2, ENERGY_BLK)
        # print(tot_int)

        for field in ENERGY_BLK:
            p.append(influxdb_client.Point(field).tag("energy", "fwd").field('fwd', fwd_int[field]))
            p.append(influxdb_client.Point(field).tag("energy", "rev").field('rev', rev_int[field]))
            p.append(influxdb_client.Point(field).tag("energy", "tot").field('tot', tot_int[field]))
            jsonout['points']['energy_fwd'][field]=fwd_int[field]
            jsonout['points']['energy_rev'][field]=rev_int[field]
            jsonout['points']['energy_tot'][field]=tot_int[field]
        
        jsonout['time'] = time.time()
        jsonout['mID'] = "bret"
            
                    
        dbwrite.write(bucket=bucket, org=org, record=p)

        reports.append(jsonout)
        reportCnt += 1
        

        print(f'{time.time()-start} - {reportCnt}')
        
        if reportCnt > 1:

            xmitrpts = {'data': reports}
            print(json.dumps(xmitrpts,indent=2))
            reportCnt = 0
            reports = []

        time.sleep(5.0)
    except Exception as e:
        print(e)
        instr.serial.close()
        dbclient.close()
        time.sleep(1)
        dbclient = influxdb_client.InfluxDBClient(
                                                url=url,
                                                token=token,
                                                org=org
                                                )
        dbwrite = dbclient.write_api(write_options=SYNCHRONOUS)


        instr = minimalmodbus.Instrument('/dev/ttyUSB0',1,minimalmodbus.MODE_RTU,False,False)
        instr.serial.baudrate = 19200
        instr.serial.parity = minimalmodbus.serial.PARITY_EVEN
        instr.serial.stopbits = minimalmodbus.serial.STOPBITS_ONE
        instr.serial.timeout = 0.5
