#!/usr/bin/python3
# -*- coding=utf-8 -*-
import sys
import socket
import struct
import selectors
import threading
from multiprocessing import Process, Manager, Lock
# from queue import Empty
# from multiprocessing import Queue
# from collections import deque

from log import logger
from config import Config, Constant
from app.storagegw import StorageGW
from app.client import Client
from app.configserver import config_server
from app.status import StatusServer
# from app.models import session_scope


class TcpServer:
    
    def __init__(self):
        pass
    
    def accept(self, sock, mask, sel, sgw_info, lock, conf_info, version_info,
               sgw_id_list, addr_list):
        """
        @:接受连接请求
        """
        try:
            conn, addr = sock.accept()
            logger.info('Accepted connection from {}'.format(addr))
        except socket.error as e:
            logger.error('Connection error message:{}'.format(e))
        else:
            conn.setblocking(False)
            sel.register(conn, selectors.EVENT_READ, self.read)
                        
    def recvall(self, conn, sel, length,):
        """
        """
        blocks = []
        while length:
            try:
                block = conn.recv(length)
            except socket.error:
                sel.unregister(conn)
                conn.close()
                break
            else:
                length -= len(block)
                blocks.append(block)
        return b''.join(blocks)
    
    def get_msg(self, conn, sel):
        """
        """
        head_unpack = None
        body = None
        header = self.recvall(conn, sel, Constant.HEAD_LENGTH)
        fmt_head = Constant.FMT_COMMON_HEAD
        try:
            head_unpack = struct.unpack(fmt_head, header)
        except struct.error:
            pass
        else :
            total_size = head_unpack[0]
            body_size = total_size - Constant.HEAD_LENGTH
            body = self.recvall(conn, sel, body_size)
        return head_unpack, body
        
    def read(self, conn, mask, sel, sgw_info, lock, conf_info, version_info,
             sgw_id_list, addr_list):
        """
        """
        head_unpack, body = self.get_msg(conn, sel)
        try:
            logger.info("TcpServer收到的head_unpack：{}".format(head_unpack))
            logger.info("TcpServer收到的body:{}".format(body))
            self.data_handler(head_unpack, body, conn, sel, sgw_info,
                              lock, conf_info, version_info, sgw_id_list,
                              addr_list)
        except:
            logger.error('TcpServer数据处理出现错误')
                        
    def data_handler(self, head_unpack, body, conn, sel, sgw_info, lock,
                     conf_info, version_info, sgw_id_list, addr_list):
        """
        @:对收到的数据进行业务处理
        (total_size, major, minor, src_type, dst_type, src_id, dst_id, trans_id,
         sequence, command, ack_code, total, offset, count) = head_unpack 
        """
        command = head_unpack[9]
         
        if command == Constant.SGW_HB:
            """
            @:处理sgw心跳消息体
            """
            logger.info('收到sgw心跳消息')
            try:
                fmt = '!5IH2xII8Q'
                body_unpack = struct.unpack(fmt, body)
                logger.info("收到sgw心跳消息body:{}".format(body_unpack))
            except:
                logger.error('sgw心跳的消息体解析出错')
            else:
                sgw_id = head_unpack[5]
                storagegw = StorageGW(sgw_id, *body_unpack)
#                 storagegw.handle_hb(head_unpack, conn, sel, sgw_info, lock,
#                                     sgw_id_list, addr_list)
                t = threading.Thread(target=storagegw.handle_hb,
                                     args=(head_unpack, conn, sel, sgw_info,
                                           lock, sgw_id_list, addr_list))
                t.start()
                logger.info('当前sgw的sgw_id列表为：{}'.format(sgw_id_list))
                logger.info("sgw注册后的数据结构：{}".format(sgw_info))
             
        elif command == Constant.CLIENT_HB:
            """
            @:处理Client心跳消息
            """
            logger.info('收到Client心跳消息')
            client = Client()
#             client.handle_hb(head_unpack, body, conn, sel, conf_info,
#                              version_info)
            t = threading.Thread(target=client.handle_hb,
                                 args=(head_unpack, body, conn, sel, conf_info,
                                       version_info))
            t.start()
            
        elif command == Constant.CLIENT_UPLOAD_ROUTE:
            """
            @:处理Client转存路径请求
            """
            logger.info('收到Client转存路径请求')
            logger.info(sgw_info)
            client = Client()
            try:
                client.handle_upload(head_unpack, body, conn, sel, sgw_info,
                                     lock)
            except:
                logger.error('sgw还没有发心跳消息注册，无存储网关信息')
            
        elif command == Constant.CLIENT_UPLOAD_SUCCESS:
            """
            @:Client上报转存成功
            """
            logger.info('收到Client上报转存成功消息')
            
        elif command == Constant.CLIENT_QUERY_NUM:
            """
            @:处理Client查询记录数量的请求
            """
            logger.info('收到Client查询记录数量的请求')
            client = Client()
            client.handle_query_num(head_unpack, body, conn, sel, conf_info)
            
        elif command == Constant.CLIENT_QUERY_DATA:
            """
            @:处理Client查询数据的请求
            """
            logger.info('收到Client查询数据的请求')
            client = Client()
            client.handle_query_data1(head_unpack, body, conn, sgw_info,
                                      lock, conf_info)
                    
        elif command == Constant.REMOTE_QUERY_NUM:
            """
            @:处理RemoteMetadataServer查询记录数量的请求
            """
            logger.info('收到RemoteMetadataServer查询记录数量的请求')
            client = Client()
            client.handle_remote_query_num(head_unpack, body, conn)
        
        elif command == Constant.REMOTE_QUERY_DATA:
            """
            @:处理RemoteMetadataServer查询数据的请求
            """
            logger.info('收到RemoteMetadataServer查询数据的请求')
            client = Client()
            client.handle_remote_query_data(head_unpack, body, conn, sgw_info,
                                            lock)
            
        elif command == Constant.CLIENT_DEL:
            """
            @:处理Client删除元数据的请求
            """
            logger.info('收到Client删除元数据的请求')
            client = Client()
            client.handle_delete(head_unpack, body, conn, sgw_info, lock,
                                 conf_info)
            
        elif command == Constant.REMOTE_DEL:
            """
            @:处理RemoteMetadataServer的删除元数据的请求
            """
            logger.info('收到RemoteMetadataServer的删除元数据的请求')
            client = Client()
            client.handle_remote_del(head_unpack, body, conn, sgw_info, lock)
            
        elif command == Constant.CLIENT_CONFIG_UPGRADE:
            """
            @:处理Client配置升级
            """
            logger.info('收到Client配置升级的请求')
            client = Client()
            client.handle_config_upgrade(head_unpack, body, conn, conf_info)
            
        elif command == Constant.CLIENT_UPGRADE:
            """
            @:处理Client软件升级
            """
            logger.info('收到Client软件升级的请求')
            client = Client()
            client.handle_client_upgrade(head_unpack, body, conn, conf_info)
            
        else:
            logger.error('解析到未定义的命令字')
        
    def run(self, listener, sel, sgw_info, lock, conf_info, version_info,
            sgw_id_list, addr_list):
        conf_recv_thread = threading.Thread(target=config_server.conf_read,
                                            args=(conf_info,))
        conf_recv_thread.start()
        sel.register(listener, selectors.EVENT_READ, self.accept)
        while True:
            events = sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask, sel, sgw_info, lock, conf_info,
                         version_info, sgw_id_list, addr_list)


def _generate_srv_sock():
    """
    @:生成监听套接字并绑定到配置的ip和port
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
    sock.listen(5)
    sock.setblocking(False)
    return sock

def _get_workers():
    """
    @:获得工作进程数
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
    
if __name__ == '__main__':
    lock = Lock()
    m = Manager()
    version_info = m.dict()
    sgw_info = m.dict()
    sgw_id_list = m.list()
    addr_list = m.list()
    # sgw_info = {2: [10240000000, [deque([(3232252929, 8000, 1000),
    #                                      (3232252930, 8000, 1001),
    #                                      (3232252931, 8000, 1002)]), 1, 1, 1]]}
    conf_info = m.dict()
    config_server.send_hb()
    conf_recv_thread = threading.Thread(target=config_server.conf_read,
                                        args=(conf_info,))
    conf_recv_thread.start()
    
    status_server = StatusServer()
    status_server.send_hb()
    
    listener = _generate_srv_sock()
    workers = _get_workers()
    sel = selectors.DefaultSelector()
    params = (listener, sel, sgw_info, lock, conf_info, version_info,
              sgw_id_list, addr_list)
    
    tcp_server = TcpServer()
    logger.info('Starting TCP services...')
    logger.info('Listening at:{}'.format(listener.getsockname()))
    for i in range(workers):
        logger.info('开始启动第{0}个子进程, 总共{1}个子进程'.format(i+1, workers))
        p = Process(target=tcp_server.run, args=params)
        p.start()



    
            
    
        



















    
    
