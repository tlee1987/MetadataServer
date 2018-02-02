#!/usr/bin/python3
# -*- coding=utf-8 -*-
import socket
import struct
import json
import sys
import time
import threading
import queue
import psutil

from log import logger
from config import Config, Constant
print('我是configserver模块，我在导入的时候被执行')


class ConfigServer:
    
    def __init__(self):
        self.major = Constant.MAJOR_VERSION
        self.minor = Constant.MINOR_VERSION
        self.src_type = Constant.METADATA_TYPE
        self.dst_type = Constant.CONFIG_TYPE
        self.src_id = int(Config.src_id, 16)
        self.dst_id = int(Config.config_dst_id, 16)
        
        self.sock = self._generate_conf_sock()
        
    def _generate_conf_sock(self):
        HOST = Config.config_server_ip
        try:
            PORT = int(Config.config_server_port)
        except:
            logger.error('配置文件meta.ini中的config_server【section】port参数配置为非数字，'
                         '请检查配置文件。')
            sys.exit()
        ADDR = (HOST, PORT)
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(ADDR)
            logger.info('连接至ConfigServer的地址为{}'.format(ADDR))
        except OSError:
            logger.error('该请求的地址{}无效，与ConfigServer连接失败，退出程序'.format(ADDR))
#             sys.exit()
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
        @:生成发送给ConfigServer的心跳消息
        """
        logger.info('生成发送给Config Server的心跳消息')
        d = self.get_info()
        body_json = json.dumps(d)
        body_pack = body_json.encode('utf-8')
        
        total = count = len(body_pack)
        total_size = total + Constant.HEAD_LENGTH
        command = Constant.CONFIG_HB
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, self.src_id, self.dst_id, command,
                  total, count]
        fmt_head = '!I4BII16xI4xQ8xI4x'
        head_pack = struct.pack(fmt_head, *header)
        logger.info("发送给ConfigServer的心跳header:{}".format(header))
        data = head_pack + body_pack
        return data
    
    def send_hb_timer(self):
        """
        @:构造发心跳消息的定时器
        """
        data = self.generate_hb()
        self.sock.sendall(data)
        global timer
        timer = threading.Timer(Constant.TIME, self.send_hb_timer)
        timer.start()
     
    def send_hb(self):
        """
        @:定时发心跳消息
        """    
        timer = threading.Timer(0, self.send_hb_timer)
        timer.start()
        
    def generate_msg(self, site_id):
        """
        @:生成发给ConfigServer查询client相关信息的消息
        @:消息体json格式：
        {"site_id": site_id}
        """
        logger.info('执行generate_msg，生成向ConfigServer查询的信息')
        body = {}
        body['site_id'] = site_id
        body_json = json.dumps(body)
        body_pack = body_json.encode('utf-8')
        
        total = count = len(body_pack)
        total_size = Constant.HEAD_LENGTH + total
        command = Constant.CONFIG_QUERY
        trans_id = site_id          # 将Client的site_id放在trans_id字段
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, self.src_id, self.dst_id, trans_id,
                  command, total, count]
        fmt_head = '!I4BIIQ8xI4xQ8xI4x'
        head_pack = struct.pack(fmt_head, *header)
        data = head_pack + body_pack
        return data
    
    def send_msg(self, site_id):
        """
        @:给ConfigServer发查询client相关信息的消息
        """
        logger.info('执行send_msg， 向ConfigServer发送查询 ')
        req = self.generate_msg(site_id)
        self.sock.sendall(req)
        
#     def recv_msg(self):
#         """
#         :解析ConfigServer回复的消息
#         """
#         logger.info('解析ConfigServer回复的消息')
#         data = b''
#         while True:
#             more = self.sock.recv(Constant.BUFSIZE)
#             if not more:
#                 break
#             data += more
#             while True:
#                 if len(data) < Constant.HEAD_LENGTH:
#                     logger.warning('接收到的数据包长度为{}，小于消息头部长度64，'
#                                    '继续接收数据'.format(len(data)))
#                     break
#                 headpack = struct.unpack(Constant.FMT_COMMON_HEAD,
#                                          data[:Constant.HEAD_LENGTH])
#                 total_size = headpack[0]
#                 if len(data) < total_size:
#                     logger.warning('接收到的数据包长度为{}，总共为{}，数据包不完整，'
#                                    '继续接收数据'.format(len(data), total_size))
#                     break
#                 body = data[Constant.HEAD_LENGTH: total_size]
# #                 try:
# #                     logger.info(headpack)
# #                     self.data_handler(headpack, body)
# #                 except:
# #                     logger.error('对Config Server的数据处理出现错误')
# #                     sys.exit()
# #                 else:
# #                     data = data[total_size:]
#                 return headpack, body
    
    def recvall(self, length):
        """
        @:接收ConfigServer回复的消息
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
        @:解析ConfigServer回复的消息
        """
        header = self.recvall(Constant.HEAD_LENGTH)
        fmt_head = Constant.FMT_COMMON_HEAD
        head_unpack = struct.unpack(fmt_head, header)
        total_size = head_unpack[0]
        body_size = total_size - Constant.HEAD_LENGTH
        body = self.recvall(body_size)
        return head_unpack, body
        
    def data_handler(self, queue_recv, conf_info):
        """
        body_unpack:
        {str(site_id): ["region_id1", "region_id2", "region_id3"...],# 历史的region_id
        "region_id": [region_id, meta_ip, meta_port, src_id],   # 当前环境配置的region_id
        "region_id1": [region_id1, meta_ip, meta_port, src_id],
        "region_id2": [region_id2, meta_ip, meta_port, src_id],
        "region_id3": [region_id3, meta_ip, meta_port, src_id],
        "config_version": config_version,
        "client_version": client_version,
        "file_url": file_url,
        "file_name": file_name,
        "file_md5": file_md5}
        """
        logger.info('执行data_handler,接收ConfigServer返回的消息')
        while True:
            head_unpack, body = self.get_msg()
            trans_id = head_unpack[7]
            site_id = trans_id
            key = str(site_id)
            command = head_unpack[9]
            
            if command == Constant.CONFIG_HB_RESP:
                logger.info('收到ConfigServer心跳回复消息')
                logger.info("ConfigServer回复的心跳消息头部：{}".format(head_unpack))
            elif command == Constant.CONFIG_QUERY_RESP:
                """
                @:将str(site_id)为key，放到接收队列中，同时以str(site_id)为key,收到的查询消息
                @:放到字典conf_info中。
                """
                logger.info('收到ConfigServer回复的查询消息')
                body_unpack = json.loads(body.decode('utf-8'))
                conf_info[key] = body_unpack
                queue_recv.put(key)
            elif command == Constant.CONFIG_INFO:
                logger.info('配置有变更，ConfigServer主动下发通知')
                body_unpack = json.loads(body.decode('utf-8'))
                site_id_info = key + '_info'
                conf_info[site_id_info] = body_unpack
                queue_recv.put(site_id_info)


config_server = ConfigServer()
    
if __name__ == '__main__':
    conf_info = {}
    queue_recv = queue.Queue()
    config_server.send_hb()
    conf_recv_thread = threading.Thread(target=config_server.data_handler,
                                        args=(queue_recv, conf_info))
    conf_recv_thread.start()
    site_id = 3
    for i in range(Constant.try_times):
        logger.info('第{}次发送查询信息'.format(i+1))
        config_server.send_msg(site_id)
        key = str(site_id)
        res = queue_recv.get(key)
        if res:
            body_unpack = conf_info.get(key)
            logger.info(body_unpack)
            conf_info.pop(key)
            break
        else:
            logger.info('延时1秒再发送查询请求')
            time.sleep(1)
    else:
        logger.error('查询3次未查到{}的配置信息'.format(site_id))

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        