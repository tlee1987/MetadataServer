#!/usr/bin/python3
# -*- coding=utf-8 -*-
import sys
import socket
import struct
import selectors
# from multiprocessing import Process, Manager

from log import logger
from config import Config, Constant
from app.storagegw import StorageGW
from app.client import Client
# from app.configserver import ConfigServer

client_config_info = {}

class TcpServer:
    
    def __init__(self):
        self.sock = self._generate_sock()
        self.workers = self._get_workers()
        self.sel = selectors.DefaultSelector()
        
        self.sgw_info = {}
        
    def _generate_sock(self):
        """
        :生成监听套接字并绑定到配置的ip和port
        """
        HOST = Config.listening_ip
        try:
            PORT = int(Config.listening_port)
        except ValueError:
            logger.error('配置文件meta.ini中的listening【section】port参数配置'
                         '为非数字，请检查配置文件。')
            sys.exit()
        ADDR = (HOST, PORT)
      
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(ADDR)
        return sock

    def _get_workers(self):
        """
        :获得工作进程数
        """
        try:
            workers = int(Config._workers)
        except ValueError:
            logger.error('配置文件meta.ini中的worker参数配置为非数字，请检查配置文件。')
            sys.exit()
        else:
            if workers <= 0 or workers > Constant.DEFAULT_WORKERS:
                workers = Constant.DEFAULT_WORKERS
        return workers       
        
    def accept(self, sock):
        """
        :接受连接请求
        """
        try:
            conn, addr = sock.accept()
            logger.info('Accepted connection from {}'.format(addr))
        except socket.gaierror as e:
            logger.error('Connection error message:{}'.format(e))
        else:
            conn.setblocking(False)
            self.sel.register(conn, selectors.EVENT_READ, self.read)
        
    def read(self, conn):
        """
        :解析收到的数据
        """
        logger.info('解析收到的数据...')
        data = b''
        while True:
            try:
                more = conn.recv(Constant.BUFSIZE)
            except OSError:
                continue
            else:
                if not more:
                    self.sel.unregister(conn)
                    break
                data += more
                while True:
                    if len(data) < Constant.HEAD_LENGTH:
                        logger.warning('接收到的数据包长度为{}，小于消息头部长度64，'
                                       '继续接收数据'.format(len(data)))
                        break
                    headpack = struct.unpack(Constant.FMT_COMMON_HEAD,
                                             data[:Constant.HEAD_LENGTH])
                    total_size = headpack[0]
    
                    if len(data) < total_size:
                        logger.warning('接收到的数据包长度为{}，总共为{}，数据包不完整，'
                                       '继续接收数据'.format(len(data), total_size))
                        break
                    body = data[Constant.HEAD_LENGTH: total_size]
                    try:
                        logger.info(headpack)
                        self.data_handler(headpack, body, conn, self.sel)
                    except:
                        logger.error('数据处理出现错误')
                        break
                    else:
                        data = data[total_size:]
                        
    def data_handler(self, headpack, body, conn, sel):
        """
        :对收到的数据进行业务处理
        (total_size, major, minor, src_type, dst_type, src_id, dst_id, trans_id,
         sequence, command, ack_code, total, offset, count) = headpack 
        """
        command = headpack[9]
         
        if command == Constant.SGW_HB:
            """
            :处理sgw心跳消息体
            """
            logger.info('处理sgw心跳消息，解析后存入数据库')
            try:
                fmt = '!6I8Q'
                bodypack = struct.unpack(fmt, body)
            except:
                logger.error('sgw心跳的消息体解析出错')
            else:
                storagegw = StorageGW(*bodypack)
                flag = storagegw.handle_hb(headpack, conn, sel)
                if flag:
                    self.sgw_info = storagegw.register_sgw(conn)
                else:
                    logger.error('sgw心跳消息有误，sgw注册失败')
             
        elif command == Constant.CLIENT_HB:
            """
            :处理Client心跳消息
            """
            logger.info('处理Client心跳消息，解析后存入数据库')
            client = Client()
            client.handle_hb(headpack, conn, *bodypack)
            
        elif command == Constant.CLIENT_UPLOAD_ROUTE:
            """
            :处理Client转存路径请求
            """
            logger.info('处理Client转存路径请求')
            client = Client()
            try:
                self.metadata_info = client.handle_upload(headpack, body, conn,
                                                          self.sgw_info)
            except:
                logger.error('sgw还没有发心跳消息注册，无存储网关信息')
            
        elif command == Constant.CLIENT_UPLOAD_SUCCESS:
            """
            :处理Client上报转存成功的消息
            """
            logger.info('处理Client上报转存成功的消息')
            client = Client()
            client.handle_upload_success(body, self.metadata_info)
            
        elif command == Constant.CLIENT_QUERY_NUM:
            """
            :处理Client查询记录数量的请求
            """
            logger.info('处理Client查询记录数量的请求')
            client = Client()
            client.handle_query_num(headpack, body, conn)
            
        elif command == Constant.CLIENT_QUERY_DATA:
            """
            :处理Client查询数据的请求
            """
            logger.info('处理Client查询数据的请求')
            client = Client()
            client.handle_query_data(headpack, body, conn, self.sgw_info)
                    
        elif command == Constant.REMOTE_METADATA_QUERY_NUM:
            """
            :处理RemoteMetadataServer查询记录数量的请求
            """
            logger.info('处理RemoteMetadataServer查询记录数量的请求')
            client = Client()
            client.handle_remote_query_num(headpack, body, conn)
        
        elif command == Constant.REMOTE_METADATA_QUERY_DATA:
            """
            :处理RemoteMetadataServer查询数据的请求
            """
            logger.info('处理RemoteMetadataServer查询数据的请求')
            client = Client()
            client.handle_remote_query_data(headpack, body, conn, self.sgw_info)
            
        elif command == Constant.CLIENT_DELETE:
            """
            :处理Client删除元数据的请求
            """
            logger.info('处理Client删除元数据的请求')
            client = Client()
            client.handle_delete(headpack, body, conn)
            
        elif command == Constant.REMOTE_METADATA_DEL:
            """
            :处理RemoteMetadataServer的删除元数据的请求
            """
            logger.info('处理RemoteMetadataServer的删除元数据的请求')
            client = Client()
            client.handle_remote_del(headpack, body, conn)
            
        elif command == Constant.CLIENT_CONFIG_UPGRADE:
            """
            :处理Client配置升级
            """
            logger.info('处理Client配置升级的请求')
            client = Client()
            client.handle_config_upgrade(headpack, body, conn)
            
        elif command == Constant.CLIENT_UPGRADE:
            """
            :处理Client软件升级
            """
            logger.info('处理Client软件升级的请求')
            client = Client()
            client.handle_client_upgrade(headpack, body, conn)
            
#         elif command == Constant.CLIENT_BLOCK_READ:
#             logger.info('处理client分块读取最新版本软件的请求')
#             
#         elif command == Constant.CLIENT_BLOCK_READ_END:
#             logger.info('client上报读取最新版本软件完成的消息')
        else:
            logger.error('解析到未定义的命令字或响应码')
        
    def run(self):
        self.sock.listen(10)
        self.sock.setblocking(False)
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)
        while True:
            events = self.sel.select()
            for key, _ in events:
                callback = key.data
                callback(key.fileobj)
    

def main():
#     config_server = ConfigServer()
#     config_server.send_msg()
#     bodyPack = config_server.data_handler()
#     if bodyPack:
#         config_version = bodyPack.get('config_version')
#         client_version = bodyPack.get('client_version')
#         client_config_info['config_version'] = config_version
#         client_config_info['client_version'] = client_version
    logger.info('Starting TCP services...')
    tcp_server = TcpServer()
    tcp_server.run()
#     for _ in range(tcp_server.workers):
#         p = Process(target=tcp_server.run)
#         p.start()
    
if __name__ == '__main__':
    main()
    

    
            
    
        



















    
    
