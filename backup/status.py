#!/usr/bin/python3
# -*- coding=utf-8 -*-
import socket
import threading
import struct
import json
import psutil
import sys

from config import Config, Constant
from log import logger
 
HOST = Config.status_server_ip
try:
    PORT = int(Config.status_server_port)
except:
    logger.error('配置文件meta.ini中的status_server【section】port参数配置为非数字，请检查配置文件。')
    sys.exit()
ADDR = (HOST, PORT)
 
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    sock.connect(ADDR)
except OSError:
    logger.error('该请求的地址{}无效，与Stats Server连接失败。'.format(ADDR))
#     sys.exit()

major = Constant.MAJOR_VERSION
minor = Constant.MINOR_VERSION
src_type = Constant.METADATA_TYPE
dst_type = Constant.STATUS_TYPE
src_id = int(0x08000000)
dst_id = int(0x0a000000)      # Status Server 
command = int(0x00030001)
fmt = '!I4BII16xI4xQ8xI4x512s'

infoDict = {}

def get_info():
    infoDict['cpu_percent'] = psutil.cpu_percent(1)
    infoDict['mem_info'] = list(psutil.virtual_memory())
    infoDict['disk_info'] = list(psutil.disk_usage('/'))
    return infoDict

def generate_hb():
    d = get_info()
    ms_info = json.dumps(d)
    total = count =  len(ms_info)
    length = total + Constant.HEAD_LENGTH
    data = struct.pack(fmt, length, major, minor, src_type, dst_type, src_id,
                       dst_id, command, total, count, ms_info.encode('utf-8'))
    return data

def run_timer():
    data = generate_hb()
#     sock.sendall(data)
    print(data)
    global timer
    timer = threading.Timer(5.0, run_timer)
    timer.start()
     
def send_hb():     
    timer = threading.Timer(0, run_timer)
    timer.start()

if __name__ == '__main__':
    send_hb()
   
   
   
    
