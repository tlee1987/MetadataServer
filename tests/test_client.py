#!/usr/bin/python3
# -*- coding=utf-8 -*-
import socket
import time
import struct
import json
import threading
import psutil

from config import Constant, Config
from log import logger

class TestClient:
    
    def __init__(self):
        self.major = Constant.MAJOR_VERSION
        self.minor = Constant.MINOR_VERSION
        self.src_type = Constant.CLIENT_TYPE
        self.dst_type = Constant.METADATA_TYPE
        self.src_id = int(0x00000001)
        self.dst_id = int(Config.src_id, 16)
        
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
        site_id    client_disk_total    client_disk_free    config_version
        client_version    transactions_num    timestamp
        """
        disk_info = list(psutil.disk_usage('/'))[1:3]
        disk_used = disk_info[0]
        disk_free = disk_info[1]
        
        site_id = 1
        client_disk_total = disk_used
        client_disk_free = disk_free
        config_version = 1
        client_version = 1
        transactions_num = 10
        timestamp = int(time.time())
        body = [site_id, client_disk_total, client_disk_free, config_version,
                client_version, transactions_num, timestamp]
        return body
    
    def generate_hb(self):
        body = self.get_info()
        fmt_body = '!IQQHHII'
        body_pack = struct.pack(fmt_body, *body)
        body_size = len(body_pack)
        
        total_size = Constant.HEAD_LENGTH + body_size
        command = Constant.CLIENT_HB
        
        global trans_id
        global sequence
        fmt_head = '!I4BIIQQI28x'
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, self.src_id, self.dst_id, trans_id,
                  sequence, command]
        head_pack = struct.pack(fmt_head, *header)
        trans_id += 1
        sequence += 1
        
        data = head_pack + body_pack
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
    
    def upload(self):
        region_id = 2
        site_id = 1
        app_id = 1
        timestamp = int(time.time())
        file_md5_tmp = '0'
        file_name_tmp = 'test3@' + str(timestamp) + '.tar'
        
        user_id = 1
        customer_id = 1
        metadata = {}
        metadata['user_id'] = user_id
        metadata['customer_id'] = customer_id
        metadata_json = json.dumps(metadata)
        metadata_pack = metadata_json.encode('utf-8')
        metadata_len = len(metadata_pack)
        
        fmt_body = '!2xH3I28x33s512sH'
        file_md5 = file_md5_tmp.encode('utf-8')
        file_name = file_name_tmp.encode('utf-8')
        body = (region_id, site_id, app_id, timestamp, file_md5,
                file_name, metadata_len)
        body_pack = struct.pack(fmt_body, *body)
        body_size = len(body_pack)
        
        total_size = Constant.HEAD_LENGTH + body_size + metadata_len
        command = Constant.CLIENT_UPLOAD_ROUTE
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, self.src_id, self.dst_id, trans_id,
                  sequence, command]
        fmt_head = '!I4BIIQQI28x'
        head_pack = struct.pack(fmt_head, *header)
        
        data = head_pack + body_pack + metadata_pack
        self.sock.sendall(data)
    
    def query_num(self):
        """
        {"site_id": [id1, id2, ...],
        "app_id": [id1, id2, ...],
        "user_id": [id1, id2, ...],
        "customer_id": [id1, id2, ...],
        "timestamp": [start_time, end_time],
        "order_by": [keyword1, keyword2…],
        "desc": bool}
        """
        site_id = [1]
        app_id = [1]
        user_id = [1]
        customer_id = [1]
        timestamp = [0, int(time.time())]
        order_by = ["timestamp"]
        desc = False
        
        query_body = {}
        query_body['site_id'] = site_id
        query_body['app_id'] = app_id
        query_body['user_id'] = user_id
        query_body['customer_id'] = customer_id
        query_body['timestamp'] = timestamp
        query_body['order_by'] = order_by
        query_body['desc'] = desc
        
        query_body_json = json.dumps(query_body)
        query_body_pack = query_body_json.encode('utf-8')
        body_size = len(query_body_pack)
        
        total_size = Constant.HEAD_LENGTH + body_size
        command = Constant.CLIENT_QUERY_NUM
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, self.src_id, self.dst_id, trans_id,
                  sequence, command]
        fmt_head = '!I4BIIQQI28x'
        head_pack = struct.pack(fmt_head, *header)
        
        data = head_pack + query_body_pack
        self.sock.sendall(data)
        
    
    def data_handler(self):    
        while True:
            headpack, body = self.get_msg()
            command = headpack[9]
            if command == Constant.CLIENT_HB_RESP:
                logger.info('收到回复的心跳消息')
                logger.info(headpack)
                logger.info(body)
                
            elif command == Constant.CLIENT_UPLOAD_ROUTE_RESP:
                logger.info('收到回复转存请求的消息')
                logger.info('收到回复转存请求的消息的head:{}'.format(headpack))
                logger.info('收到回复转存请求的消息的body:{}'.format(body))
                
            elif command == Constant.CLIENT_QUERY_NUM_RESP:
                logger.info('收到回复查询记录数量的消息')
                logger.info('收到回复查询记录数量的消息的head:{}'.format(headpack))
                logger.info('收到回复查询记录数量的消息body:{}'.format(body))
                
            else:
                logger.error('使用了错误的命令字')   
                
                
        
if __name__ == '__main__':
    trans_id = 0
    sequence = 0
    test_client = TestClient()        
#     test_client.send_hb()
    t = threading.Thread(target=test_client.data_handler)
    t.start()
#     test_client.upload()
    test_client.query_num()
        
        
        
        
        
        
        
        
        
        
        
        
        
    