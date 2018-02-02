#!/usr/bin/python3
# -*- coding=utf-8 -*-
import socket
import sys
import struct

from log import logger
from config import Config, Constant


class RemoteMetadata:
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
        self.sock = self.generate_sock()
        
    def generate_sock(self):
        ADDR = (self.host, self.port)
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(ADDR)
        except OSError:
            logger.error('该请求的地址{}无效，与Remote Metadata'
                         'Server连接失败'.format(ADDR))
#             sys.exit()
        return sock
    
    def generate_msg(self, headpack, body):
        """
        LENGTH    MAJOR    MINOR    src_type    dst_type    src_id    dst_id    
        TRANS_ID    SEQUENCE    COMMAND    ACK_CODE    total    OFFSET    count
        pad
        """
        (total_size, major, minor, src_type, dst_type, src_id, dst_id,
         trans_id, sequence, command, ack_code, total, offset, count) = headpack
         
        major = Constant.MAJOR_VERSION
        minor = Constant.MINOR_VERSION
        src_type = Constant.METADATA_TYPE
        dst_type = Constant.METADATA_TYPE
        src_id = int(0x08000000)
        dst_id = int(0x08000001)
        
        header = [total_size, major, minor, src_type, dst_type, src_id, dst_id,
                  trans_id, sequence, command, ack_code, total, offset, count]
        headPack = struct.pack(Constant.FMT_COMMON_HEAD, *header)
        data = headPack + body
        return data
        
    def send_msg(self, headpack, body):
        data = self.generate_msg(headpack, body)
        self.sock.sendall(data)
        
    def recv_msg(self):
        pass
            
    def handle_query(self):
        pass
            
            
            
    
        