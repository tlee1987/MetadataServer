#!/usr/bin/python3
# -*- coding=utf-8 -*-
import sys
import socket
import struct
import selectors
import threading
from collections import deque
from multiprocessing import Process, Manager, Lock

from log import logger
from config import Config, Constant
from app.storagegw import StorageGW
from app.client import Client
from app.configserver import config_server
from app.status import StatusServer

      
class TcpServer:
       
    def __init__(self):
        pass
       
    def accept(self, sock, sel):
        """
        @:接受连接请求
        """
        try:
            conn, addr = sock.accept()
            logger.info('Accepted connection from: {}'.format(addr))
        except socket.error as e:
            logger.error('Connection error message: {}'.format(e))
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
            except OSError:
                pass
#                 sel.unregister(conn)
#                 conn.close()
#                 break
            else:
                length -= len(block)
                blocks.append(block)
        return b''.join(blocks)
       
    def read(self, conn, sel):
        """
        """
        fmt_head = Constant.FMT_COMMON_HEAD
        try:
            header = self.recvall(conn, sel, Constant.HEAD_LENGTH)
            head_unpack = struct.unpack(fmt_head, header)
        except struct.error:
            sel.unregister(conn)
            conn.close()
        except:
            sel.unregister(conn)
            conn.close()
        else:
            total_size = head_unpack[0]
            body_size = total_size - Constant.HEAD_LENGTH
            try:
                body = self.recvall(conn, sel, body_size)
                logger.info("TcpServer收到的head_unpack:{}".format(head_unpack))
                logger.info("TcpServer收到的body:{}".format(body))
                self.data_handler(head_unpack, body, conn, sel)
            except:
                logger.error('TcpServer数据处理出现错误')
                sel.unregister(conn)
                conn.close()
       
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
                t1 = threading.Thread(target=storagegw.handle_hb,
                                      args=(head_unpack, conn, sel, sgw_info,
                                            lock, sgw_id_info, addr_list))
                t1.start()
                logger.info('当前sgw的sgw_id信息为：{}'.format(sgw_id_info))
                logger.info('当前sgw的addr_list列表为：{}'.format(addr_list))
                logger.info("sgw注册后的数据结构：{}".format(sgw_info))
                
        elif command == Constant.CLIENT_HB:
            """
            @:处理Client心跳消息
            """
            logger.info('收到Client心跳消息')
            client = Client()
            t2 = threading.Thread(target=client.handle_hb,
                                 args=(head_unpack, body, conn, sel,
                                       conf_info, version_info))
            t2.start()
               
        elif command == Constant.CLIENT_UPLOAD_ROUTE:
            """
            @:处理Client转存路径请求
            """
            logger.info('收到Client转存路径请求')
            logger.info(sgw_info)
            client = Client()
            try:
                t3 = threading.Thread(target=client.handle_upload,
                                     args=(head_unpack, body, conn, sel, sgw_info,
                                           lock))
                t3.start()
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
            t4 = threading.Thread(target=client.handle_query_num,
                                  args=(head_unpack, body, conn, sel, conf_info))
            t4.start()
            
        elif command == Constant.FILE_QUERY_NUM:
            """
            @:处理Fileportal查询记录数量的请求
            """
            logger.info('收到Fileportal查询记录数量的请求')
            client = Client()
            t13 = threading.Thread(target=client.handle_file_query_num,
                                  args=(head_unpack, body, conn, sel))
            t13.start()
               
        elif command == Constant.CLIENT_QUERY_DATA:
            """
            @:处理Client查询数据的请求
            """
            logger.info('收到Client查询数据的请求')
            client = Client()
            t5 = threading.Thread(target=client.handle_query_data,
                                  args=(head_unpack, body, conn, sel, sgw_info,
                                        lock, conf_info))
            t5.start()
                
        elif command == Constant.FILE_QUERY_DATA:
            """
            @:处理Fileportal查询数据的请求
            """
            logger.info('收到Fileportal查询数据的请求')
            client = Client()
            t12 = threading.Thread(target=client.handle_file_query_data,
                                  args=(head_unpack, body, conn, sel, sgw_info,
                                        lock))
            t12.start()
                       
        elif command == Constant.REMOTE_QUERY_NUM:
            """
            @:处理RemoteMetadataServer查询记录数量的请求
            """
            logger.info('收到RemoteMetadataServer查询记录数量的请求')
            client = Client()
            t6 = threading.Thread(target=client.handle_remote_query_num,
                                  args=(head_unpack, body, conn, sel))
            t6.start()
           
        elif command == Constant.REMOTE_QUERY_DATA:
            """
            @:处理RemoteMetadataServer查询数据的请求
            """
            logger.info('收到RemoteMetadataServer查询数据的请求')
            client = Client()
            t7 = threading.Thread(target=client.handle_remote_query_data,
                                  args=(head_unpack, body, conn, sel, sgw_info,
                                        lock))
            t7.start()
               
        elif command == Constant.CLIENT_DEL:
            """
            @:处理Client删除元数据的请求
            """
            logger.info('收到Client删除元数据的请求')
            client = Client()
            t8 = threading.Thread(target=client.handle_delete,
                                  args=(head_unpack, body, conn, sel, sgw_info,
                                        lock, conf_info))
            t8.start()
            
        elif command == Constant.FILE_DEL:
            """
            @:处理Client删除元数据的请求
            """
            logger.info('收到Fileportal删除元数据的请求')
            client = Client()
            t14 = threading.Thread(target=client.handle_file_delete,
                                  args=(head_unpack, body, conn, sel, sgw_info,
                                        lock))
            t14.start()
               
        elif command == Constant.REMOTE_DEL:
            """
            @:处理RemoteMetadataServer的删除元数据的请求
            """
            logger.info('收到RemoteMetadataServer的删除元数据的请求')
            client = Client()
            t9 = threading.Thread(target=client.handle_remote_del,
                                  args=(head_unpack, body, conn, sel, sgw_info,
                                        lock))
            t9.start()
               
        elif command == Constant.CLIENT_CONFIG_UPGRADE:
            """
            @:处理Client配置升级
            """
            logger.info('收到Client配置升级的请求')
            client = Client()
            t10 = threading.Thread(target=client.handle_config_upgrade,
                                   args=(head_unpack, body, conn, sel, conf_info))
            t10.start()
               
        elif command == Constant.CLIENT_UPGRADE:
            """
            @:处理Client软件升级
            """
            logger.info('收到Client软件升级的请求')
            client = Client()
            t11 = threading.Thread(target=client.handle_client_upgrade,
                                   args=(head_unpack, body, conn, sel, conf_info))
            t11.start()
        else:
            logger.error('解析到未定义的命令字')
          
    def run(self):
#         conf_recv_thread1 = threading.Thread(target=config_server.conf_read,
#                                              args=(conf_info,))
#         conf_recv_thread1.start()
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
        sock.listen(100)
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

def run(listener, sel, tcp_server):
    sel.register(listener, selectors.EVENT_READ, tcp_server.accept)
    while True:
        events = sel.select()
        for key, _ in events:
            callback = key.data
            callback(key.fileobj, sel)
    
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
    @:sgw_id_info,保存sgw的sgw_id信息，用于判断是否将sgw的信息写入数据库中的sgw_static表，也用于判断是否掉线
    @:数据结构为：{sgw_id1: timestamp,
                sgw_id2: timestamp,
                sgw_id3: timestamp, ...}
    @:addr_list,保存sgw的addr信息,用于更新sgw_info
    @:数据结构为：[(self.listen_ip1, self.listen_port1, sgw_id1), ...]
    @:conf_info,用于向ConfigServer查询时，保存ConfigServer回复的信息
    @:数据结构为：{str(id): query_body_unpack, ...}
     
    """
    lock = Lock()
    m = Manager()
    version_info = m.dict()
    sgw_info = m.dict()
    sgw_id_info = m.dict()
    client_id_info = m.dict()
    addr_list = m.list()
    sgw_info = {2: [1024000, [deque([(3232252929, 8000, 1000),
                                     (3232252930, 8000, 1001),
                                     (3232252931, 8000, 1002)]), 1, 1, 2]]}
    conf_info = m.dict()
    conf_recv_thread = threading.Thread(target=config_server.conf_read,
                                        args=(conf_info,))
    conf_recv_thread.start()
 
    conf_sendhb_timer = threading.Thread(target=config_server.send_hb)
    conf_sendhb_timer.start()
    
    status_server = StatusServer()
    status_server_timer = threading.Thread(target=status_server.send_hb)
    status_server_timer.start()
    
    check_sgw_hb_thread = threading.Thread(target=StorageGW.check_sgw_hb,
                                           args=(sgw_id_info, addr_list))
    check_sgw_hb_thread.start()
    
    logger.info("conf_recv_thread's name:{}".format(conf_recv_thread.name))
    logger.info("conf_recv_thread's ident:{}".format(conf_recv_thread.ident))
    logger.info("conf_recv_thread's is_alive:{}".format(conf_recv_thread.is_alive()))
    logger.info("conf_sendhb_timer's name:{}".format(conf_sendhb_timer.name))
    logger.info("conf_sendhb_timer's ident:{}".format(conf_sendhb_timer.ident))
    logger.info("conf_sendhb_timer's is_alive:{}".format(conf_sendhb_timer.is_alive()))
    logger.info("status_server_timer's name:{}".format(status_server_timer.name))
    logger.info("status_server_timer's ident:{}".format(status_server_timer.ident))
    logger.info("status_server_timer's is_alive:{}".format(status_server_timer.is_alive()))
    logger.info("check_sgw_hb_thread's name:{}".format(check_sgw_hb_thread.name))
    logger.info("check_sgw_hb_thread's ident:{}".format(check_sgw_hb_thread.ident))
    logger.info("check_sgw_hb_thread's is_alive:{}".format(check_sgw_hb_thread.is_alive()))
    
    logger.info('thread {} is running...'.format(threading.current_thread().name))
    logger.info('active_count:{}'.format(threading.active_count()))
    logger.info('main_thread:{}'.format(threading.main_thread()))
    logger.info('get_ident:{}'.format(threading.get_ident()))
     
    workers = _get_workers()
    listener = _generate_srv_sock()
    tcp_server = TcpServer()
     
    logger.info('Starting TCP services...')
    logger.info('Listening at:{}'.format(listener.getsockname()))

    for i in range(workers):
        logger.info('开始启动第{0}个子进程, 总共{1}个子进程'.format(i+1, workers))
        sel = selectors.DefaultSelector()
        p = Process(target=run, args=(listener, sel, tcp_server))
        p.start()
        





    
