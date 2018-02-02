#!/usr/bin/python3
# -*- coding=utf-8 -*-

HEAD_LENGTH = 64
MAJOR_VERSION = 1
MINOR_VERSION = 0

METADATA_TYPE = int(0x02)
CONFIG_TYPE = int(0x05)
# CLIENT_TYPE = int(0x01)
# SGW_TYPE = int(0x03)
# STATUS_TYPE = int(0x04)


CONFIG_QUERY = int(0x00040001)
CONFIG_HB = int(0x00040003)
ACK_CONFIG_QUERY = int(0x00040002)
ACK_CONFIG_HB = int(0x00040004)
CONFIG_INFO = int(0x00040005)

FMT_COMMON_HEAD = '!I4BIIQQIIQQI4x' # struct模块的用法
"""
启TCP服务
解析收到的数据，拿出head和body
业务逻辑判断处理

构造头部
(total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
 trans_id, sequence, command, ack_code, total, offset, count) = headpack
 
fmt_head = Constant.FMT_COMMON_HEAD
header = [total_size, self.major, self.minor, self.src_type,
          self.dst_type, src_id, dst_id, trans_id, sequence, command,
          ack_code, total, offset, count]
headPack = struct.pack(fmt_head, *header)

构造消息体 json


"""
import socket

conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)



