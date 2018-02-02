#!/usr/bin/python3
# -*- coding=utf-8 -*-
import os
import configparser

print('我是config模块，我在导入的时候被执行')
basedir = os.path.abspath(os.path.dirname(__file__))
meta_path = os.path.join(basedir, 'meta.ini')

conf = configparser.ConfigParser()
conf.read(meta_path)

class Config:
    """
    mysql_db1:mysql+pymysql://root:anyun100@localhost:3306/test
    mysql_db2:mysql+pymysql://root:123456@192.168.68.43:3306/test
    """
    db_conn_str = conf.get('mysql_db1', 'conn_str')
    
    region_id = int(conf.get('local_config', 'region_id'))
    system_id = int(conf.get('local_config', 'system_id'))
    src_id = conf.get('local_config', 'src_id')
    status_dst_id = conf.get('local_config', 'status_dst_id')
    config_dst_id = conf.get('local_config', 'config_dst_id')
    _workers = conf.get('local_config', 'workers')
    
    status_server_ip = conf.get('status_server', 'ip')
    status_server_port = conf.get('status_server', 'port')
    
    config_server_ip = conf.get('config_server', 'ip')
    config_server_port = conf.get('config_server', 'port')
    
    listening_ip = conf.get('listening', 'ip')
    listening_port = conf.get('listening', 'port')
    
class Constant:
    HEAD_LENGTH = 64
    TASKINFO_FIXED_LENGTH = 591
    MAJOR_VERSION = 1
    MINOR_VERSION = 0
    CLIENT_TYPE = int(0x01)
    METADATA_TYPE = int(0x02)
    SGW_TYPE = int(0x03)
    STATUS_TYPE = int(0x04)
    CONFIG_TYPE = int(0x05)
    
    CPU_NUMS = os.cpu_count()
#     DEFAULT_WORKERS = int(3/4*CPU_NUMS)
    DEFAULT_WORKERS = int(CPU_NUMS)
    BUFSIZE = 2048
    METADATA_VERSION = 1
#     TIME = 5.0
    TIME = 20.0
    DELAYED = 15.0
    try_times = 3
    
    FMT_COMMON_HEAD = '!I4BIIQQIIQQI4x'
    FMT_TASKINFO_FIXED = '!HHIIIHH4IQ33s512sH'
    FMT_TASKINFO_SEND = '!2xHIIIHH4I8x33x512sH'
    # client与metadata server通信 命令字
    CLIENT_UPLOAD_ROUTE = int(0x00000001)
    CLIENT_UPLOAD_ROUTE_RESP = int(0x00000002)
    CLIENT_UPLOAD_SUCCESS = int(0x00000003)
    CLIENT_QUERY_NUM = int(0x00000004)
    CLIENT_QUERY_NUM_RESP = int(0x00000005)
    CLIENT_QUERY_DATA = int(0x00000006)
    CLIENT_QUERY_DATA_RESP = int(0x00000007)
    CLIENT_CONFIG_UPGRADE = int(0x00000008)
    CLIENT_CONFIG_UPGRADE_RESP = int(0x00000009)
    CLIENT_UPGRADE = int(0x0000000A)
    CLIENT_UPGRADE_RESP = int(0x0000000B)
    CLIENT_DEL = int(0x0000000C)
    CLIENT_DEL_RESP = int(0x0000000D)
    CLIENT_HB = int(0x00001000)
    CLIENT_HB_RESP = int(0x00001001)
    
    # client与metadata server通信 响应码
    ACK_CLIENT_UPLOAD_ROUTE = int(0x00000100)
    ACK_CLIENT_UPLOAD_ROUTE_NOTFOUND = int(0x00000101)
    ACK_CLIENT_QUERY_NUM = int(0x00000300)
    ACK_CLIENT_QUERY_DATA = int(0x00000400)
    ACK_CLIENT_CONFIG_UPGRADE = int(0x00000600)
    ACK_CLIENT_UPGRADE = int(0x00000700)
    ACK_CLIENT_DEL_SUCCESS = int(0x00000A00)
    ACK_CLIENT_DEL_FAILED = int(0x00000A01)
    ACK_CLIENT_HB = int(0x00001001)
    
    # sgw与metadata server通信命令字
    SGW_HB = int(0x00010001)
    SGW_HB_RESP = int(0x00010002)
    ACK_SGW_HB = int(0x00010003)
    
    # metadata server与status server通信命令字
    METADATA_HB = int(0x00030001)
    
    # metadata server与config server通信命令字与响应码
    CONFIG_QUERY = int(0x00040001)
    CONFIG_QUERY_RESP = int(0x00040002)
    CONFIG_HB = int(0x00040003)
    CONFIG_HB_RESP = int(0x00040004)
    CONFIG_INFO = int(0x00040005)
    
    ACK_CONFIG_QUERY = int(0x00040006)
    ACK_CONFIG_HB = int(0x00040007)
    
    # metadata server与remote metadata server通信命令字
    REMOTE_QUERY_NUM = int(0x00050001)
    REMOTE_QUERY_NUM_RESP = int(0x00050002)
    REMOTE_QUERY_DATA = int(0x00050003)
    REMOTE_QUERY_DATA_RESP = int(0x00050004)
    REMOTE_DEL = int(0x00050005)
    REMOTE_DEL_RESP = int(0x00050006)
    ACK_REMOTE_QUERY_NUM = int(0x00050100)
    ACK_REMOTE_QUERY_DATA = int(0x00050300)
    ACK_REMOTE_DEL_SUCCESS = int(0x00050500)
    ACK_REMOTE_DEL_FAILED = int(0x00050501)
    

print('mysql_db1', Config.db_conn_str)
print(conf.get('mysql_db2', 'conn_str'))
