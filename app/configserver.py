#!/usr/bin/python3
# -*- coding=utf-8 -*-
import json
import socket
import struct
import sys
import threading
import time
import psutil

from config import Config, Constant
from log import logger

# from multiprocessing import Process

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
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            logger.error('Created config socket Failed:{}'.format(e))
            
        try:
            sock.connect(ADDR)
            logger.info('连接至ConfigServer的地址为{}'.format(ADDR))
        except socket.error:
            logger.error('该请求的地址{}无效，与ConfigServer连接失败'.format(ADDR))
            sock.close()
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
        try:
            self.sock.sendall(data)
        except socket.error:
            self.sock.close()
        else:
            global timer
            timer = threading.Timer(Constant.TIME, self.send_hb_timer)
            timer.start()
            flag = timer.is_alive()
            logger.info('config timer is alive:{}'.format(flag))
            logger.info("config timer's name is:{}".format(timer.name))
            logger.info("config timer's ident is:{}".format(timer.ident))

    def send_hb(self):
        """
        @:定时发心跳消息
        """
        timer = threading.Timer(Constant.TIME, self.send_hb_timer)
        timer.start()
        logger.info("!config timer's name is:{}".format(timer.name))
        logger.info("!config timer's ident is:{}".format(timer.ident))

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
        logger.info("向ConfigServer查询的消息的head:{}".format(header))
        logger.info("向ConfigServer查询的消息的body:{}".format(body_json))
        data = head_pack + body_pack
        return data

    def send_msg(self, site_id):
        """
        @:给ConfigServer发查询client相关信息的消息
        """
        logger.info('执行send_msg， 向ConfigServer发送查询 ')
        req = self.generate_msg(site_id)
        try:
            self.sock.sendall(req)
        except socket.error:
            self.sock.close()

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
            try:
                block = self.sock.recv(length)
            except:
                self.sock.close()
                break
            else:
                length -= len(block)
                blocks.append(block)
        return b''.join(blocks)

    def get_msg(self):
        """
        @:解析ConfigServer回复的消息
        """
        header = self.recvall(Constant.HEAD_LENGTH)
        fmt_head = Constant.FMT_COMMON_HEAD
        try:
            head_unpack = struct.unpack(fmt_head, header)
        except struct.error:
            pass
        else:
            total_size = head_unpack[0]
            body_size = total_size - Constant.HEAD_LENGTH
            body = self.recvall(body_size)
            return head_unpack, body

    def conf_read(self, conf_info):
        logger.info('执行conf_read,接收ConfigServer返回的消息')
        while True:
            time.sleep(0.1)
            try:
                head_unpack, body = self.get_msg()
            except:
                pass
            else:
                try:
                    self.data_handler(head_unpack, body, conf_info)
                except:
                    logger.info('处理ConfigServer消息错误')
                    self.sock.close()
                    break

    def data_handler(self, head_unpack, body, conf_info):
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
        logger.info('执行data_handler,处理ConfigServer返回的消息')
        trans_id = head_unpack[7]
        site_id = trans_id
        key = str(site_id)
        command = head_unpack[9]

        if command == Constant.CONFIG_HB_RESP:
            logger.info('收到ConfigServer心跳回复消息')
            logger.info("ConfigServer回复的心跳消息头部：{}".format(head_unpack))

        elif command == Constant.CONFIG_QUERY_RESP:
            """
            @:以str(site_id)为键,收到的查询消息为值放到字典conf_info中。
            """
            logger.info('收到ConfigServer回复的查询消息')
            body_unpack = json.loads(body.decode('utf-8'))
            conf_info[key] = body_unpack

        elif command == Constant.CONFIG_INFO:
            logger.info('配置有变更，ConfigServer主动下发通知')
            body_unpack = json.loads(body.decode('utf-8'))
            conf_info[key] = body_unpack

        else:
            logger.info('接受ConfigServer消息有误')


config_server = ConfigServer()

def query(conf_info, site_id):
    for i in range(Constant.try_times):
        logger.info('第{}次发送查询信息'.format(i+1))
        config_server.send_msg(site_id)
        key = str(site_id)
        body_unpack = conf_info.get(key)
        if body_unpack:
            conf_info.pop(key)
            logger.info(body_unpack)
            break
        else:
            logger.info('延时1秒发送查询请求')
            time.sleep(3)
    else:
        logger.error('查询3次未查到site_id：{}的配置信息'.format(site_id))
    
if __name__ == '__main__':
    conf_info = {}
    config_server.send_hb()
    conf_recv_thread = threading.Thread(target=config_server.conf_read,
                                        args=(conf_info,))
    conf_recv_thread.start()
#     conf_recv_p = Process(target=config_server.data_handler,
#                           args=(conf_info,))
#     conf_recv_p.start()
    site_id = 2
#     p = Process(target=query, args=(conf_info, site_id))
#     p.start()
    for i in range(Constant.try_times):
        logger.info('第{}次发送查询信息'.format(i+1))
        config_server.send_msg(site_id)
        key = str(site_id)
        body_unpack = conf_info.get(key)
        if body_unpack:
            conf_info.pop(key)
            logger.info(body_unpack)
            break
        else:
            logger.info('延时1秒发送查询请求')
            time.sleep(3)
    else:
        logger.error('查询3次未查到site_id：{}的配置信息'.format(site_id))

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        