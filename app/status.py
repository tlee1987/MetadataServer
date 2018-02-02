#!/usr/bin/python3
# -*- coding=utf-8 -*-
import socket
import threading
import struct
import json
import sys
import time
import psutil

from log import logger
from config import Config, Constant
from app.models import MetadataStatus, session_scope
print('我是status模块，我在导入的时候被执行')


class StatusServer:
    
    def __init__(self):
        self.major = Constant.MAJOR_VERSION
        self.minor = Constant.MINOR_VERSION
        self.src_type = Constant.METADATA_TYPE
        self.dst_type = Constant.STATUS_TYPE
        self.src_id = int(Config.src_id, 16)
        self.dst_id = int(Config.status_dst_id, 16)
        self.command = Constant.METADATA_HB
        
#         self.sock = self._generate_status_sock()

    def _generate_status_sock(self): 
        HOST = Config.status_server_ip
        try:
            PORT = int(Config.status_server_port)
        except:
            logger.error('配置文件meta.ini中的status_server【section】port参数配置为非数字，'
                        '请检查配置文件。')
            sys.exit()
        ADDR = (HOST, PORT)
         
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(ADDR)
            logger.info('连接至StatusServer的地址为{}'.format(ADDR))
        except OSError:
            logger.error('该请求的地址{}无效，与Stats Server连接失败。'.format(ADDR))
            sys.exit()
        return sock
    
    def get_info(self):
        """
        @:生成MetadataServer的状态信息
        region_id    system_id    meta_version    cpu_percent    mem_used
        mem_free    disk_uesd    disk_free    netio_input    netio_output
        timestamp
        """
        cpu_percent = int(psutil.cpu_percent())
        mem_info = list(psutil.virtual_memory())[-2:]
        disk_info = list(psutil.disk_usage('/'))[1:3]
        netio_info = list(psutil.net_io_counters())[:2]
        
        region_id = Config.region_id
        system_id = Config.system_id
        meta_version = Constant.METADATA_VERSION
        mem_used = mem_info[0]
        mem_free = mem_info[1]
        disk_used = disk_info[0]
        disk_free = disk_info[1]
        netio_input = netio_info[1]
        netio_output = netio_info[0]
        timestamp = int(time.time())
        
        self.metadata_status = MetadataStatus(region_id=region_id,
                                              system_id=system_id,
                                              meta_version=meta_version,
                                              cpu_percent=cpu_percent,
                                              mem_used=mem_used,
                                              mem_free=mem_free,
                                              disk_used=disk_used,
                                              disk_free=disk_free,
                                              netio_input=netio_input,
                                              netio_output=netio_output,
                                              timestamp=timestamp)
        
        infoDict = {}
        infoDict['region_id'] = region_id
        infoDict['system_id'] = system_id
        infoDict['meta_version'] = meta_version
        infoDict['cpu_percent'] = cpu_percent
        infoDict['mem_used'] = mem_used
        infoDict['mem_free'] = mem_free
        infoDict['disk_used'] = disk_used
        infoDict['disk_free'] = disk_free
        infoDict['netio_input'] = netio_input
        infoDict['netio_output'] = netio_output
        infoDict['timestamp'] = timestamp
        return infoDict   
    
    def generate_hb(self):
        """
        @:生成发送给StatusServer的心跳消息
        """
        d = self.get_info()
        body = json.dumps(d)
        total = count = len(body.encode('utf-8'))
        length = total + Constant.HEAD_LENGTH
        header = [length, self.major, self.minor, self.src_type, self.dst_type,
                  self.src_id, self.dst_id, self.command, total, count]
        fmt_head = '!I4BII16xI4xQ8xI4x'
        head_pack = struct.pack(fmt_head, *header)
        data = head_pack + body.encode('utf-8')
        return data

    def send_hb_timer(self):
        """
        @:构造往数据库写状态消息的定时器
        """
#         data = self.generate_hb()
#         self.sock.sendall(data)
        self.get_info()
        with session_scope() as session:
            session.add(self.metadata_status)
        global timer
        timer = threading.Timer(Constant.TIME, self.send_hb_timer)
        timer.start()
     
    def send_hb(self):
        """
        @:定时写状态信息
        """       
        timer = threading.Timer(Constant.TIME, self.send_hb_timer)
        timer.start()


if __name__ == '__main__':
    status_server = StatusServer()
    status_server.send_hb()
    
    
    
    
    
    
    
    
    
    
    
    
    