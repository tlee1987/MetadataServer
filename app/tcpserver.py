#!/usr/bin/python3
# -*- coding=utf-8 -*-
import sys
import time
import socket
import struct
import selectors
import threading
from collections import deque
from multiprocessing import Process, Manager, Lock
# from queue import Empty
# from multiprocessing import Queue

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
       
    def accept(self, sock, sel):
        """
        @:接受连接请求
        """
        try:
            conn, addr = sock.accept()
            logger.info('Accepted connection from {}'.format(addr))
        except socket.error as e:
#             pass
            logger.error('Connection error message:{}'.format(e))
        else:
            conn.setblocking(False)
            sel.register(conn, selectors.EVENT_READ, self.read)
                           
    def recvall(self, conn, sel, length):
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
                if not block:
                    break
                length -= len(block)
                blocks.append(block)
        return b''.join(blocks)
       
    def get_msg(self, conn, sel):
        """
        """
        header = self.recvall(conn, sel, Constant.HEAD_LENGTH)
        fmt_head = Constant.FMT_COMMON_HEAD
        try:
            head_unpack = struct.unpack(fmt_head, header)
        except struct.error:
            pass
        else:
            total_size = head_unpack[0]
            body_size = total_size - Constant.HEAD_LENGTH
            body = self.recvall(conn, sel, body_size)
            return head_unpack, body
           
    def read(self, conn, sel):
        """
        """
        try:
            head_unpack, body = self.get_msg(conn, sel)
        except:
            pass
        else:
            try:
                logger.info("TcpServer收到的head_unpack：{}".format(head_unpack))
                logger.info("TcpServer收到的body:{}".format(body))
                self.data_handler(head_unpack, body, conn, sel)
            except:
                logger.error('TcpServer数据处理出现错误')
       
    def data_handler(self, head_unpack, body, conn, sel):
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
                sel.unregister(conn)
                conn.close()
            else:
                sgw_id = head_unpack[5]
                storagegw = StorageGW(sgw_id, *body_unpack)
#                 storagegw.handle_hb(head_unpack, conn, sgw_info, lock,
#                                     sgw_id_list, addr_list)
                t = threading.Thread(target=storagegw.handle_hb,
                                     args=(head_unpack, conn, sgw_info,
                                           lock, sgw_id_list, addr_list))
                t.start()
                logger.info('当前sgw的sgw_id列表为：{}'.format(sgw_id_list))
                logger.info('当前sgw的addr_list列表为：{}'.format(addr_list))
                logger.info("sgw注册后的数据结构：{}".format(sgw_info))
                
        elif command == Constant.CLIENT_HB:
            """
            @:处理Client心跳消息
            """
            logger.info('收到Client心跳消息')
            client = Client()
            try:
#                 client.handle_hb(head_unpack, body, conn, conf_info,
#                                  version_info)
                t = threading.Thread(target=client.handle_hb,
                                     args=(head_unpack, body, conn,
                                           conf_info, version_info))
                t.start()
            except:
                sel.unregister(conn)
                conn.close()
               
        elif command == Constant.CLIENT_UPLOAD_ROUTE:
            """
            @:处理Client转存路径请求
            """
            logger.info('收到Client转存路径请求')
            logger.info(sgw_info)
            client = Client()
            try:
                client.handle_upload(head_unpack, body, conn, sgw_info,
                                     lock)
            except:
                logger.error('sgw还没有发心跳消息注册，无存储网关信息')
                sel.unregister(conn)
                conn.close()
               
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
            try:
                client.handle_query_num(head_unpack, body, conn, conf_info)
            except:
                sel.unregister(conn)
                conn.close()
               
        elif command == Constant.CLIENT_QUERY_DATA:
            """
            @:处理Client查询数据的请求
            """
            logger.info('收到Client查询数据的请求')
            client = Client()
            try:
                client.handle_query_data(head_unpack, body, conn, sgw_info,
                                         lock, conf_info)
            except:
                sel.unregister(conn)
                conn.close()
                       
        elif command == Constant.REMOTE_QUERY_NUM:
            """
            @:处理RemoteMetadataServer查询记录数量的请求
            """
            logger.info('收到RemoteMetadataServer查询记录数量的请求')
            client = Client()
            try:
                client.handle_remote_query_num(head_unpack, body, conn)
            except:
                sel.unregister(conn)
                conn.close()
           
        elif command == Constant.REMOTE_QUERY_DATA:
            """
            @:处理RemoteMetadataServer查询数据的请求
            """
            logger.info('收到RemoteMetadataServer查询数据的请求')
            client = Client()
            try:
                client.handle_remote_query_data(head_unpack, body, conn,
                                                sgw_info, lock)
            except:
                sel.unregister(conn)
                conn.close()
               
        elif command == Constant.CLIENT_DEL:
            """
            @:处理Client删除元数据的请求
            """
            logger.info('收到Client删除元数据的请求')
            client = Client()
            try:
                client.handle_delete(head_unpack, body, conn, sgw_info, lock,
                                     conf_info)
            except:
                sel.unregister(conn)
                conn.close()
               
        elif command == Constant.REMOTE_DEL:
            """
            @:处理RemoteMetadataServer的删除元数据的请求
            """
            logger.info('收到RemoteMetadataServer的删除元数据的请求')
            client = Client()
            try:
                client.handle_remote_del(head_unpack, body, conn, sgw_info,
                                         lock)
            except:
                sel.unregister(conn)
                conn.close()
               
        elif command == Constant.CLIENT_CONFIG_UPGRADE:
            """
            @:处理Client配置升级
            """
            logger.info('收到Client配置升级的请求')
            client = Client()
            try:
                client.handle_config_upgrade(head_unpack, body, conn, conf_info)
            except:
                sel.unregister(conn)
                conn.close()
               
        elif command == Constant.CLIENT_UPGRADE:
            """
            @:处理Client软件升级
            """
            logger.info('收到Client软件升级的请求')
            client = Client()
            try:
                client.handle_client_upgrade(head_unpack, body, conn, conf_info)
            except:
                sel.unregister(conn)
                conn.close()
        else:
            logger.error('解析到未定义的命令字')
          
    def run(self):
#         conf_recv_thread = threading.Thread(target=config_server.conf_read,
#                                             args=(conf_info,))
#         conf_recv_thread.start()
        sel = selectors.DefaultSelector()
        sel.register(listener, selectors.EVENT_READ, self.accept)
        while True:
            events = sel.select()
            for key, _ in events:
                callback = key.data
                callback(key.fileobj, sel)
                

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
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as e:
        logger.error('Create socket Failed:{}'.format(e))
        sys.exit()
    
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except socket.error as e:
        logger.error('Setsockopt SO_REUSEADDR Failed:{}'.format(e))
        sys.exit()
    
    try:
        sock.bind(ADDR)
    except socket.error as e:
        logger.error('Bind Failed:{}'.format(e))
        sys.exit()
        
    try:
        sock.listen(5)
    except socket.error as e:
        logger.error('Listen Failed:{}'.format(e))
        sys.exit()
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
    """
    @:version_info,保存Client版本信息,用于回复Client心跳
    @:数据结构为：{str(site_id): {'config_version': config_version,
                              'client_version': client_version}}
    @:sgw_info,保存sgw信息，用于给Client轮询选择存储网关地址
    @:数据结构为：
    @:{group_id1: [disk_free1, [deque[addr1, addr2, ...], region_id, system_id, group_id1]],
       group_id2: [disk_free2, [deque[addr3, addr4, ...], region_id, system_id, group_id2]],
        ...}
       addr = (ip, port, sgw_id)
    @:sgw_id_list,保存sgw的sgw_id信息，用于判断是否将sgw的信息写入数据库中的sgw_static表
    @:数据结构为：[sgw_id1, sgw_id2, sgw_id3, ...]
    @:addr_list,保存sgw的addr信息,用于更新sgw_info
    @:数据结构为：[(self.listen_ip1, self.listen_port1, sgw_id1), ...]
    @:conf_info,用于向ConfigServer查询时，保存ConfigServer回复的信息
    @:数据结构为：{str(id): query_body_unpack, ...}
     
    """
    lock = Lock()
    m = Manager()
    version_info = m.dict()
    sgw_info = m.dict()
    sgw_id_list = m.list()
    addr_list = m.list()
    sgw_info = {2: [1024000, [deque([(3232252929, 8000, 1000),
                                     (3232252930, 8000, 1001),
                                     (3232252931, 8000, 1002)]), 1, 1, 2]]}
    conf_info = m.dict()
    
    config_server.send_hb()
    conf_recv_thread = threading.Thread(target=config_server.conf_read,
                                        args=(conf_info,))
    conf_recv_thread.start()
    logger.info("conf_recv_thread's name:{}".format(conf_recv_thread.name))
    logger.info("conf_recv_thread's ident:{}".format(conf_recv_thread.ident))
    logger.info("conf_recv_thread's is_alive:{}".format(conf_recv_thread.is_alive()))
    
    logger.info('thread {} is running...'.format(threading.current_thread().name))
    logger.info('active_count:{}'.format(threading.active_count()))
    logger.info('main_thread:{}'.format(threading.main_thread()))
    logger.info('get_ident:{}'.format(threading.get_ident()))
     
    status_server = StatusServer()
    status_server.send_hb()
     
    workers = _get_workers()
    listener = _generate_srv_sock()
#     params = (listener,)
     
    tcp_server = TcpServer()
    logger.info('Starting TCP services...')
    logger.info('Listening at:{}'.format(listener.getsockname()))
    tcp_server.run()

#     for i in range(workers):
#         logger.info('开始启动第{0}个子进程, 总共{1}个子进程'.format(i+1, workers))
#         p = Process(target=tcp_server.run)
#         p.start()
        
#     while True:
#         time.sleep(10)
#         logger.info('*'*30)
#         logger.info('conf_recv_thread is alive:{}'.format(conf_recv_thread.is_alive()))
#         logger.info('*'*30)
#         if not conf_recv_thread.is_alive():
#             conf_recv_thread.start()



# class TcpServer:
#        
#     def __init__(self):
#         pass
#        
#     def accept(self, sock):
#         """
#         @:接受连接请求
#         """
#         try:
#             conn, addr = sock.accept()
#             logger.info('Accepted connection from {}'.format(addr))
#         except socket.error:
#             pass
# #             logger.error('Connection error message:{}'.format(e))
#         else:
#             conn.setblocking(False)
#             sel.register(conn, selectors.EVENT_READ, self.read)
#                            
#     def recvall(self, conn, length):
#         """
#         """
#         blocks = []
#         while length:
#             try:
#                 block = conn.recv(length)
#             except socket.error:
#                 sel.unregister(conn)
#                 conn.close()
#                 break
#             else:
#                 if not block:
#                     break
#                 length -= len(block)
#                 blocks.append(block)
#         return b''.join(blocks)
#        
#     def get_msg(self, conn):
#         """
#         """
#         header = self.recvall(conn, Constant.HEAD_LENGTH)
#         fmt_head = Constant.FMT_COMMON_HEAD
#         try:
#             head_unpack = struct.unpack(fmt_head, header)
#         except struct.error:
#             pass
#         else:
#             total_size = head_unpack[0]
#             body_size = total_size - Constant.HEAD_LENGTH
#             body = self.recvall(conn, body_size)
#             return head_unpack, body
#            
#     def read(self, conn):
#         """
#         """
#         try:
#             head_unpack, body = self.get_msg(conn)
#         except:
#             pass
#         else:
#             try:
#                 logger.info("TcpServer收到的head_unpack：{}".format(head_unpack))
#                 logger.info("TcpServer收到的body:{}".format(body))
#                 self.data_handler(head_unpack, body, conn)
#             except:
#                 logger.error('TcpServer数据处理出现错误')
#        
#     def data_handler(self, head_unpack, body, conn):
#         """
#         @:对收到的数据进行业务处理
#         (total_size, major, minor, src_type, dst_type, src_id, dst_id, trans_id,
#          sequence, command, ack_code, total, offset, count) = head_unpack 
#         """
#         command = head_unpack[9]
#             
#         if command == Constant.SGW_HB:
#             """
#             @:处理sgw心跳消息体
#             """
#             logger.info('收到sgw心跳消息')
#             try:
#                 fmt = '!5IH2xII8Q'
#                 body_unpack = struct.unpack(fmt, body)
#                 logger.info("收到sgw心跳消息body:{}".format(body_unpack))
#             except:
#                 logger.error('sgw心跳的消息体解析出错')
#                 sel.unregister(conn)
#                 conn.close()
#             else:
#                 sgw_id = head_unpack[5]
#                 storagegw = StorageGW(sgw_id, *body_unpack)
# #                 storagegw.handle_hb(head_unpack, conn, sgw_info, lock,
# #                                     sgw_id_list, addr_list)
#                 t = threading.Thread(target=storagegw.handle_hb,
#                                      args=(head_unpack, conn, sgw_info,
#                                            lock, sgw_id_list, addr_list))
#                 t.start()
#                 logger.info('当前sgw的sgw_id列表为：{}'.format(sgw_id_list))
#                 logger.info('当前sgw的addr_list列表为：{}'.format(addr_list))
#                 logger.info("sgw注册后的数据结构：{}".format(sgw_info))
#                 
#         elif command == Constant.CLIENT_HB:
#             """
#             @:处理Client心跳消息
#             """
#             logger.info('收到Client心跳消息')
#             client = Client()
#             try:
# #                 client.handle_hb(head_unpack, body, conn, conf_info,
# #                                  version_info)
#                 t = threading.Thread(target=client.handle_hb,
#                                      args=(head_unpack, body, conn,
#                                            conf_info, version_info))
#                 t.start()
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#                
#         elif command == Constant.CLIENT_UPLOAD_ROUTE:
#             """
#             @:处理Client转存路径请求
#             """
#             logger.info('收到Client转存路径请求')
#             logger.info(sgw_info)
#             client = Client()
#             try:
#                 client.handle_upload(head_unpack, body, conn, sgw_info,
#                                      lock)
#             except:
#                 logger.error('sgw还没有发心跳消息注册，无存储网关信息')
#                 sel.unregister(conn)
#                 conn.close()
#                
#         elif command == Constant.CLIENT_UPLOAD_SUCCESS:
#             """
#             @:Client上报转存成功
#             """
#             logger.info('收到Client上报转存成功消息')
#                
#         elif command == Constant.CLIENT_QUERY_NUM:
#             """
#             @:处理Client查询记录数量的请求
#             """
#             logger.info('收到Client查询记录数量的请求')
#             client = Client()
#             try:
#                 client.handle_query_num(head_unpack, body, conn, conf_info)
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#                
#         elif command == Constant.CLIENT_QUERY_DATA:
#             """
#             @:处理Client查询数据的请求
#             """
#             logger.info('收到Client查询数据的请求')
#             client = Client()
#             try:
#                 client.handle_query_data(head_unpack, body, conn, sgw_info,
#                                          lock, conf_info)
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#                        
#         elif command == Constant.REMOTE_QUERY_NUM:
#             """
#             @:处理RemoteMetadataServer查询记录数量的请求
#             """
#             logger.info('收到RemoteMetadataServer查询记录数量的请求')
#             client = Client()
#             try:
#                 client.handle_remote_query_num(head_unpack, body, conn)
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#            
#         elif command == Constant.REMOTE_QUERY_DATA:
#             """
#             @:处理RemoteMetadataServer查询数据的请求
#             """
#             logger.info('收到RemoteMetadataServer查询数据的请求')
#             client = Client()
#             try:
#                 client.handle_remote_query_data(head_unpack, body, conn,
#                                                 sgw_info, lock)
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#                
#         elif command == Constant.CLIENT_DEL:
#             """
#             @:处理Client删除元数据的请求
#             """
#             logger.info('收到Client删除元数据的请求')
#             client = Client()
#             try:
#                 client.handle_delete(head_unpack, body, conn, sgw_info, lock,
#                                      conf_info)
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#                
#         elif command == Constant.REMOTE_DEL:
#             """
#             @:处理RemoteMetadataServer的删除元数据的请求
#             """
#             logger.info('收到RemoteMetadataServer的删除元数据的请求')
#             client = Client()
#             try:
#                 client.handle_remote_del(head_unpack, body, conn, sgw_info,
#                                          lock)
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#                
#         elif command == Constant.CLIENT_CONFIG_UPGRADE:
#             """
#             @:处理Client配置升级
#             """
#             logger.info('收到Client配置升级的请求')
#             client = Client()
#             try:
#                 client.handle_config_upgrade(head_unpack, body, conn, conf_info)
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#                
#         elif command == Constant.CLIENT_UPGRADE:
#             """
#             @:处理Client软件升级
#             """
#             logger.info('收到Client软件升级的请求')
#             client = Client()
#             try:
#                 client.handle_client_upgrade(head_unpack, body, conn, conf_info)
#             except:
#                 sel.unregister(conn)
#                 conn.close()
#         else:
#             logger.error('解析到未定义的命令字')
#           
#     def run(self):
#         conf_recv_thread = threading.Thread(target=config_server.conf_read,
#                                             args=(conf_info,))
#         conf_recv_thread.start()
#         
#         while True:
#             events = sel.select()
#             for key, _ in events:
#                 callback = key.data
#                 callback(key.fileobj)
#                 
# 
# def _generate_srv_sock():
#     """
#     @:生成监听套接字并绑定到配置的ip和port
#     """
#     HOST = Config.listening_ip
#     try:
#         PORT = int(Config.listening_port)
#     except ValueError:
#         logger.error('配置文件meta.ini中的listening【section】port参数配置'
#                      '为非数字，请检查配置文件。')
#         sys.exit()
#     ADDR = (HOST, PORT)
#     
#     try:
#         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     except socket.error as e:
#         logger.error('Create socket Failed:{}'.format(e))
#         sys.exit()
#     
#     try:
#         sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#     except socket.error as e:
#         logger.error('Setsockopt SO_REUSEADDR Failed:{}'.format(e))
#         sys.exit()
#     
#     try:
#         sock.bind(ADDR)
#     except socket.error as e:
#         logger.error('Bind Failed:{}'.format(e))
#         sys.exit()
#         
#     try:
#         sock.listen(5)
#     except socket.error as e:
#         logger.error('Listen Failed:{}'.format(e))
#         sys.exit()
#     sock.setblocking(False)
#     return sock
# 
# def _get_workers():
#     """
#     @:获得工作进程数
#     """
#     try:
#         workers = int(Config._workers)
#     except ValueError:
#         logger.error('配置文件meta.ini中的worker参数配置为非数字，请检查配置文件。')
#         sys.exit()
#     else:
#         if workers <= 0 or workers > Constant.DEFAULT_WORKERS:
#             workers = Constant.DEFAULT_WORKERS
#     return workers   
#     
# if __name__ == '__main__':
#     """
#     @:version_info,保存Client版本信息,用于回复Client心跳
#     @:数据结构为：{str(site_id): {'config_version': config_version,
#                               'client_version': client_version}}
#     @:sgw_info,保存sgw信息，用于给Client轮询选择存储网关地址
#     @:数据结构为：
#     @:{group_id1: [disk_free1, [deque[addr1, addr2, ...], region_id, system_id, group_id1]],
#        group_id2: [disk_free2, [deque[addr3, addr4, ...], region_id, system_id, group_id2]],
#         ...}
#        addr = (ip, port, sgw_id)
#     @:sgw_id_list,保存sgw的sgw_id信息，用于判断是否将sgw的信息写入数据库中的sgw_static表
#     @:数据结构为：[sgw_id1, sgw_id2, sgw_id3, ...]
#     @:addr_list,保存sgw的addr信息,用于更新sgw_info
#     @:数据结构为：[(self.listen_ip1, self.listen_port1, sgw_id1), ...]
#     @:conf_info,用于向ConfigServer查询时，保存ConfigServer回复的信息
#     @:数据结构为：{str(id): query_body_unpack, ...}
#      
#     """
#     lock = Lock()
#     m = Manager()
#     version_info = m.dict()
#     sgw_info = m.dict()
#     sgw_id_list = m.list()
#     addr_list = m.list()
#     sgw_info = {2: [1024000, [deque([(3232252929, 8000, 1000),
#                                      (3232252930, 8000, 1001),
#                                      (3232252931, 8000, 1002)]), 1, 1, 2]]}
#     conf_info = m.dict()
#     config_server.send_hb()
#     conf_recv_thread = threading.Thread(target=config_server.conf_read,
#                                         args=(conf_info,))
#     conf_recv_thread.start()
#     logger.info('thread {} is running...'.format(threading.current_thread().name))
#     logger.info('active_count:{}'.format(threading.active_count()))
#     logger.info('main_thread:{}'.format(threading.main_thread()))
#     logger.info('get_ident:{}'.format(threading.get_ident()))
#     logger.info("conf_recv_thread's name:{}".format(conf_recv_thread.name))
#     logger.info("conf_recv_thread's ident:{}".format(conf_recv_thread.ident))
#     logger.info("conf_recv_thread's is_alive:{}".format(conf_recv_thread.is_alive()))
#      
#     status_server = StatusServer()
#     status_server.send_hb()
#      
#     sel = selectors.DefaultSelector()
#     workers = _get_workers()
#     listener = _generate_srv_sock()
#      
#     params = (listener,)
#      
#     tcp_server = TcpServer()
#     logger.info('Starting TCP services...')
#     logger.info('Listening at:{}'.format(listener.getsockname()))
# 
#     sel.register(listener, selectors.EVENT_READ, tcp_server.accept)
# #     tcp_server.run()
#     for i in range(workers):
#         logger.info('开始启动第{0}个子进程, 总共{1}个子进程'.format(i+1, workers))
#         p = Process(target=tcp_server.run)
#         p.start()
#         
# #     while True:
# #         time.sleep(30)
# #         if not conf_recv_thread.is_alive():
# #             conf_recv_thread.start()












    
