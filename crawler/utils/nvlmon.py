import sys
import json
import re
from datetime import datetime

import subprocess
import StringIO
import os
import platform
import json

fd = None
file_opened = None

def emit(emit_duration, emit_location, timestamp, emit_obj):
    global fd
    global file_opened

    filename = emit_location + "/" + timestamp

    if int(timestamp) % int(emit_duration) == 0:
        if fd:
            fd.close()
	    os.rename(file_opened+".tmp",file_opened)
            fd = None

    if not fd:
        fd = open(filename+".tmp","w")
        file_opened = filename

    fd.write(json.dumps(emit_obj))
    fd.write("\n")


def emit_json(emit_duration, emit_location, num_gpu, timestamp, pcie_arr, nvl_arr):
    gpu_bw_stats = {}
    gpu_bw_stats['timestamp'] = timestamp
    
    for i in range(num_gpu):
        pcie_stats = pcie_arr[i].split(',')
        nvl_stats = nvl_arr[i].split(',')
        gpuID = str(i+1)

        gpu_bw_stats['PCIeRx-'+gpuID] = pcie_stats[1]    # ex: pcie_arr[0] = ',0.000000,0.000000'
        gpu_bw_stats['PCIeTx-'+gpuID] = pcie_stats[2]

        gpu_bw_stats['NVDevRx-'+gpuID] = nvl_stats[1]    # ex: nvl_arr[0] = ',0.0,0.0,0.0,0.0177812576294
        gpu_bw_stats['NVDevTx-'+gpuID] = nvl_stats[2]
        gpu_bw_stats['NVH2D-'+gpuID] = nvl_stats[3]
        gpu_bw_stats['NVD2H-'+gpuID] = nvl_stats[4]

    emit(emit_duration, emit_location, timestamp, gpu_bw_stats)


def parse_raw_gpu(emit_duration='30', emit_location="/tmp/gpu_bw_stats"):
    '''
    Read gpu data from nvlmon utility and return individual CSV strings
    Format: time_sec,PCIeRx-1,PCIeTx-1,NVDevRx-1,NVDevTx-1,NVH2D-1,NVD2H-1,PCIeRx-2,PCIeTx-2,NVDevRx-2,NVDevTx-2,NVH2D-2,NVD2H-2,PCIeRx-3,PCIeTx-3,NVDevRx-3,NVDevTx-3,NVH2D-3,NVD2H-3,PCIeRx-4,PCIeTx-4,NVDevRx-4,NVDevTx-4,NVH2D-4,NVD2H-4
    '''
    time0 = False
    topology = 0
    num_gpu = 0
    num_nvlink = 0
    gpu_str = '0'
    membw_str = '0'
    mem_str = '0'
    pow_str = '0'
    temp_str = '0'
    pcie_str = [None] * 128
    nvl_str = [None] * 128
    int_str = [None] * 128
    for i in range(128): # assume no larger than 128 gpu pre cluster 
        pcie_str[i] = "" 
        nvl_str[i] = ""
        int_str[i] = '0'
    len0 = 0

    prevtime=0

    nvl_bin = os.getcwd() + "/utils/nvidialib/lib-ppc64/" + "nvlmon_ppc64le_p9 -9 -d 1 -e -p"

    while True:
        line = subprocess.check_output(nvl_bin, shell=True)

        try:
            item = line.split(',')
            time = item[0]
            etime = 1

            if not time0:
                len0 = len(item)
                time0 = time
                num_gpu = int(item[1])
                num_nvlink = item[11]
                if int(num_nvlink) == 6:  # power9, so far no power/6gpu topology data, so do not do that
                    if int(num_gpu) == 4: # power9/4gpu, supported, 0,4,8 p2p recv; 1,5,9 p2p send; 2,6,10 h2d; 3,7,11 d2h
                        #print ("Topology: Power9/4 GPUs")
                        topology = 964
                elif int(num_nvlink) == 4:  # power8
                    if int(num_gpu) == 4: # power8/4gpu, supported
                        print ("Topology: Power8/4 GPUs")
                        topology = 844
                else:  # something have nvlink but does not power8/9?
                    topology = int(num_nvlink)
            else:
                if len0 != len(item):
                    break # skip incomplete data
                etime = long(time) - long(time0)
                if etime - prevtime == 0:
                    etime = prevtime + 1
                gpu_str += '\n' + str(etime)
                membw_str += '\n' + str(etime)
                mem_str += '\n'+ str(etime)
                pow_str += '\n' + str(etime)
                temp_str += '\n' + str(etime)
                for i in range(num_gpu):
                    pcie_str[i] = ""
                    nvl_str[i] = ""
                    int_str[i] += '\n' + str(etime)
                    #pcie_str[i] += '\n' + str(etime)
                    #nvl_str[i] += '\n' + str(etime)
            # init 2, (10 + num of nvlinks fields) pre gpu (22 for p9)
            curidx = 2
            for i in range(num_gpu):
                #item[i] is name
                gpu_str += ',' + item[curidx+1]
                membw_str += ',' + item[curidx+2]
                if long(item[curidx + 4]) == long(item[curidx + 3]):
                    mem_str += ",0" # 0%
                else:
                    mem_str += ',' + str((float(item[curidx+4]) / float(item[curidx+3])) * 100)  # convert to % usage
                if long(item[curidx + 5]) > 0:
                    pow_str += ',' + str(float(item[curidx + 5]) / 1000)  # unit: w
                else:
                    pow_str += ',' + item[curidx + 5]  # 0
                temp_str += ',' + item[curidx+6]
                pcie_str[i] += ',' + item[curidx+7] + ',' + item[curidx+8]
                # item[curidx + 9] / item[11] is the number of nvlinks
                NVLIDX = curidx+10
                if topology == 964:  # power9/4gpu, supported, 0,4,8 p2p recv; 1,5,9 p2p send; 2,6,10 h2d; 3,7,11 d2h
                    p2p_recv=long(item[NVLIDX]) + long(item[NVLIDX + 4]) + long(item[NVLIDX+8])
                    p2p_send=long(item[NVLIDX+1]) + long(item[NVLIDX + 5]) + long(item[NVLIDX+9])
                    h2d=long(item[NVLIDX+2]) + long(item[NVLIDX + 6]) + long(item[NVLIDX+10])
                    d2h=long(item[NVLIDX+3]) + long(item[NVLIDX + 7]) + long(item[NVLIDX+11])
                    nvl_str[i] += "," + str(float(p2p_recv) / 1024 / 1024 / (etime-prevtime))
                    nvl_str[i] += "," + str(float(p2p_send) / 1024 / 1024 / (etime-prevtime))
                    nvl_str[i] += "," + str(float(h2d) / 1024 / 1024 / (etime-prevtime))
                    nvl_str[i] += "," + str(float(d2h) / 1024 / 1024 / (etime-prevtime))
                    curidx = NVLIDX + 12
                elif topology == 844:  # power8/4gpu, supported, 0,2 p2p recv; 1,3 p2p send; 4,6 h2d; 5,7 d2h, this mapping is incorrect, FIXIT
                    p2p_recv=long(item[NVLIDX]) + long(item[NVLIDX + 2])
                    p2p_send=long(item[NVLIDX+1]) + long(item[NVLIDX + 3])
                    h2d=long(item[NVLIDX+4]) + long(item[NVLIDX + 6])
                    d2h=long(item[NVLIDX+5]) + long(item[NVLIDX + 7])
                    nvl_str[i] += "," + str(float(p2p_recv) / 1024 / 1024 / (etime-prevtime))
                    nvl_str[i] += "," + str(float(p2p_send) / 1024 / 1024 / (etime-prevtime))
                    nvl_str[i] += "," + str(float(h2d) / 1024 / 1024 / (etime-prevtime))
                    nvl_str[i] += "," + str(float(d2h) / 1024 / 1024/ (etime-prevtime))
                    curidx = NVLIDX + 8
                else: # something have nvlink but does not power8/9?, did not test yet.
                    for r in range(int(item[curidx+8])):  # get nvlinks, should be equal to num_nvlink
                        nvl_str[i] += "," + float(item[NVLIDX + r])/1024/1024/etime
                    print ("Warning: Unknown topology")
                    curidx = NVLIDX + int(item[curidx + 9]) + 1
                int_str[i] = int_str[i] + pcie_str[i] + nvl_str[i]

 		#print str(etime)+pcie_str[i]+nvl_str[i]

            prevtime = etime
            emit_json(emit_duration, emit_location, num_gpu, str(etime), pcie_str, nvl_str)
        except Exception as err:
            print(str(err))
            print(err.args)
            print(line)
            pass


if __name__ == '__main__':
    if len(sys.argv) == 3:
        parse_raw_gpu(emit_duration=sys.argv[1], emit_location=sys.argv[2])
    else:
        parse_raw_gpu()
    
