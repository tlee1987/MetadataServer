#!/usr/bin/python3
# -*- coding=utf-8 -*-
import socket
import struct
import psutil
import time
import threading

from config import Constant, Config
from log import logger


class TestSGW:
    
    def __init__(self):
        self.total_size = 152
        self.major = Constant.MAJOR_VERSION
        self.minor = Constant.MINOR_VERSION
        self.src_type = Constant.SGW_TYPE
        self.dst_type = Constant.METADATA_TYPE
        self.src_id = int(0x90000000)
        self.dst_id = int(Config.src_id, 16)
        self.command = Constant.SGW_HB
        
        self.sock = self.generate_sock()
        
    def generate_sock(self):
        HOST = '192.168.64.47'
        PORT = 3434
#         HOST = '192.168.68.43'
#         PORT = 7788
        ADDR = (HOST, PORT)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(ADDR)
        return sock
    
    def get_info(self):
        """
        region_id    system_id    group_id    sgw_version    timestamp
        cpu_percent    mem_total    mem_free    disk_used    disk_free
        netio_input    netio_output    conn_state    conn_dealed
        """
        cpu_percent = int(psutil.cpu_percent())
        mem_info = list(psutil.virtual_memory())[-2:]
        disk_info = list(psutil.disk_usage('/'))[1:3]
        netio_info = list(psutil.net_io_counters())[:2]
        
        region_id = 2
        system_id = 2
        group_id = 1
        sgw_version = 1
        timestamp = int(time.time())
        mem_total = mem_info[0]
        mem_free = mem_info[1]
        disk_used = disk_info[0]
        disk_free = disk_info[1]
        netio_input = netio_info[1]
        netio_output = netio_info[0]
        conn_state = 0
        conn_dealed = 0
        body = [region_id, system_id, group_id, sgw_version, timestamp,
                cpu_percent, mem_total, mem_free, disk_used, disk_free,
                netio_input, netio_output, conn_state, conn_dealed]
        return body
    
    def generate_hb(self):
        """
        (152, 1, 0, 3, 2, 2415919106, 2147483648, 18, 18, 65537, 0, 0, 0, 0)
        """
        global trans_id
        global sequence
        header = [self.total_size, self.major, self.minor, self.src_type,
                  self.dst_type, self.src_id, self.dst_id, trans_id,
                  sequence, self.command]
        fmt_head = '!I4BIIQQI28x'
        headpack = struct.pack(fmt_head, *header)
        trans_id += 1
        sequence += 1
        
        body =  self.get_info()
        fmt_body = '!6I8Q'
        bodypack = struct.pack(fmt_body, *body)
        logger.info(header)
        logger.info(body)
        data = headpack + bodypack
        return data
    
    def send_hb_timer(self):
        data = self.generate_hb()
        self.sock.sendall(data)
        global timer
        timer = threading.Timer(Constant.TIME, self.send_hb_timer)
        timer.start()
        
    def send_hb(self):
        timer = threading.Timer(0, self.send_hb_timer)
        timer.start()
        
    def recvall(self, length):
        """
        """
        blocks = []
        while length:
            block = self.sock.recv(length)
            if not block:
                raise EOFError
            length -= len(block)
            blocks.append(block)
        return b''.join(blocks)
    
    def get_msg(self):
        """
        """
        header = self.recvall(Constant.HEAD_LENGTH)
        fmt_head = Constant.FMT_COMMON_HEAD
        headpack = struct.unpack(fmt_head, header)
        total_size = headpack[0]
        body_size = total_size - Constant.HEAD_LENGTH
        body = self.recvall(body_size)
        return headpack, body
    
    def data_handler(self):    
        while True:
            headpack, body = self.get_msg()
            command = headpack[9]
            if command == Constant.SGW_HB_RESP:
                logger.info('回复的心跳消息')
                logger.info(headpack)
                logger.info(body)
            else:
                logger.error('使用了错误的命令字')
  
        
if __name__ == '__main__':
    trans_id = 0
    sequence = 0
    test_sgw = TestSGW()
    test_sgw.send_hb()
    t = threading.Thread(target=test_sgw.data_handler)
    t.start()
    
    
    
    
    
    
    
    
    
    
    