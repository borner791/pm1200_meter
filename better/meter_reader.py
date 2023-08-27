import minimalmodbus
import time
import struct
import json

class meter_reader:
    RMS_BLK = {'VA':'f','W':'f', 'VAR':'f','PF':'f','VLL':'f','VLN':'f','A':'f','HZ':'f','res':'l','res':'L'}
    RMS_AVG_BLK_START = 3000
    RMS_PHA_BLK_START = 3030
    RMS_PHB_BLK_START = 3060
    N_RMS_REGS = 20

    POWER_QUALITY_BLK = {'V1THD':'f','V2THD':'f','V3THD':'f','A1THD':'f','A2THD':'f'}
    POWER_QUAL_BLK_START = 3860
    N_QUALITY_REGS = 10

    ENERGY_BLK = {'VAH':'f','WH':'f', 'VARHi':'f','res':'l','res':'l','VARHc':'f','res':'l','res':'l','res':'l','Sec':'L'}
    ENERGY_FWD_BLK_START = 3121
    ENERGY_REV_BLK_START = 3151
    ENERGY_TOT_BLK_START = 3181
    N_ENERGY_REGS = 20
    
    CURRENTTOT_BLK = {'A_TOT':'f'}
    CURRENTTOT_BLK_START = 3912
    N_CURRENT_REGS = 2

    CURRENT1_BLK = {'A1':'f'}
    CURRENT1_BLK_START = 3928
    N_CURRENT_REGS = 2

    CURRENT2_BLK = {'A2':'f'}
    CURRENT2_BLK_START = 3942
    N_CURRENT_REGS = 2

    CURRENT3_BLK = {'A3':'f'}
    CURRENT3_BLK_START = 3956
    N_CURRENT_REGS = 2

    def __init__(self, device, mbaddress, baud=19200,
                parity=minimalmodbus.serial.PARITY_EVEN, 
                stops=minimalmodbus.serial.STOPBITS_ONE, 
                mode=minimalmodbus.MODE_RTU, 
                timeout = 5.0):
        self.device = device
        self.mbAddress = mbaddress
        self.baud=baud
        self.parity = parity
        self.stops=stops
        self.mode=mode
        self.timeout=timeout
        self.connects = 0
        self.connect()

    def connect(self):
        self.instr = minimalmodbus.Instrument(self.device,self.mbAddress,self.mode,debug=False)
        self.instr.serial.baudrate = self.baud
        self.instr.serial.parity = self.parity
        self.instr.serial.stopbits = self.stops
        self.instr.serial.timeout = self.timeout
        self.connects += 1
        

    def _format_register_block(self,resp, keys):
        regs_per_value = 2
        vals = {}
        idx = 0

        for reg in keys:
            cur_idx = regs_per_value*idx
            idx+=1
            packed_bytes = struct.pack("HH",resp[cur_idx],resp[cur_idx+1])
            formatcode = f'<{keys[reg]}'
            value = float(struct.unpack(formatcode, packed_bytes)[0])
            vals[reg] = value
        return vals

    def _read_register_block(self, start, n_regs, format):
        successful = False
        while not successful:
            try:
                regs = self.instr.read_registers(start,n_regs)
                successful = True
            except:
                self.instr.serial.close()
                time.sleep(1)
                self.connect()
        
        return self._format_register_block(regs, format)
    
    def read_current_block(self):
        meter_readings = {'tot':dict(),'1':dict(),'2':dict(),'3':dict()}
        meter_readings['tot'] = self._read_register_block(self.CURRENTTOT_BLK_START, self.N_CURRENT_REGS, self.CURRENTTOT_BLK)
        meter_readings['1'] = self._read_register_block(self.CURRENT1_BLK_START, self.N_RMS_REGS, self.CURRENT1_BLK)
        meter_readings['2'] = self._read_register_block(self.CURRENT2_BLK_START, self.N_RMS_REGS, self.CURRENT2_BLK)
        meter_readings['3'] = self._read_register_block(self.CURRENT3_BLK_START, self.N_RMS_REGS, self.CURRENT3_BLK)
        return meter_readings
    
    def read_rms_block(self):
        meter_readings = {'avg':dict(),'B':dict(),'A':dict()}
        meter_readings['avg'] = self._read_register_block(self.RMS_AVG_BLK_START, self.N_RMS_REGS, self.RMS_BLK)
        meter_readings['A'] = self._read_register_block(self.RMS_PHA_BLK_START, self.N_RMS_REGS, self.RMS_BLK)
        meter_readings['B'] = self._read_register_block(self.RMS_PHB_BLK_START, self.N_RMS_REGS, self.RMS_BLK)
        return meter_readings
    
    def read_quality_blk(self):
        meter_readings = {'B_Q':dict(),'A_Q':dict()}
        p_qual = self._read_register_block(self.POWER_QUAL_BLK_START, self.N_QUALITY_REGS, self.POWER_QUALITY_BLK)

        meter_readings['A_Q']['V1THD']=p_qual['V1THD']
        meter_readings['B_Q']['V2THD']=p_qual['V2THD']
        meter_readings['A_Q']['A1THD']=p_qual['A1THD']
        meter_readings['B_Q']['A2THD']=p_qual['A2THD']
        return meter_readings
    
    def read_energy_blk(self):
        meter_readings = {'energy_fwd':dict(),'energy_rev':dict(),'energy_tot':dict()}
        meter_readings['energy_fwd'] = self._read_register_block(self.ENERGY_FWD_BLK_START, 
                                                                 self.N_ENERGY_REGS, 
                                                                 self.ENERGY_BLK)        
        meter_readings['energy_rev'] = self._read_register_block(self.ENERGY_REV_BLK_START, 
                                                                   self.N_ENERGY_REGS, 
                                                                   self.ENERGY_BLK)
        meter_readings['energy_tot'] = self._read_register_block(self.ENERGY_TOT_BLK_START, 
                                                                   self.N_ENERGY_REGS, 
                                                                   self.ENERGY_BLK)

        return meter_readings



if __name__ == '__main__':
    pm1200 = meter_reader('/dev/ttyUSB0',1)

    while True:
        meter_readings=dict()
        start = time.time()
        
        meter_readings |= pm1200.read_rms_block()
        meter_readings |= pm1200.read_quality_blk()
        meter_readings |= pm1200.read_energy_blk()
        print(json.dumps(meter_readings,indent=2))
        current = pm1200.read_current_block()
        print(json.dumps(current,indent=2))


        print(f'{time.time()-start} - {pm1200.connects}')
        time.sleep(1)
        
