#!/usr/bin/python3
# -*- coding=utf-8 -*-
import struct
import json
import time
import socket
import threading
from itertools import cycle
from operator import itemgetter
from sqlalchemy import and_, desc, asc

from log import logger
from config import Config, Constant
from app.configserver import config_server
from app.models import ClientStatus, MetadataInfo, session_scope
print('我是client模块，我在导入的时候被执行')


class Client:
    
    def __init__(self):
        self.major = Constant.MAJOR_VERSION
        self.minor = Constant.MINOR_VERSION
        self.src_type = Constant.METADATA_TYPE
        self.dst_type = Constant.CLIENT_TYPE
        
    def _generate_resp_hb(self, head_unpack, site_id, queue_recv, conf_info):
        """
        @:生成client心跳回复消息
        @:回复的消息体json格式：
        {"config_version": config_version,
        "client_version": client_version}
        """
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        offset = head_unpack[12]
        
        # 生成消息体
        conf_body_unpack = self.get_conf(site_id, queue_recv, conf_info)
        if conf_body_unpack:
            config_version = conf_body_unpack.get('config_version')
            client_version = conf_body_unpack.get('client_version')
        else:
            logger.error('经过三次查询未查询到配置信息')
            config_version = 0
            client_version = 0
        
        # test
#         config_version = 1
#         client_version = 1
        body = {}
        body['config_version'] = config_version
        body['client_version'] = client_version
        body_json = json.dumps(body)
        body_pack = body_json.encode('utf-8')
        
        total = count = len(body_pack)
        total_size = Constant.HEAD_LENGTH + total
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        command = Constant.CLIENT_HB_RESP
        ack_code = Constant.ACK_CLIENT_HB
        
        # 生成头部
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, src_id, dst_id, trans_id, sequence, command,
                  ack_code, total, offset, count]
        logger.info("回复Client心跳消息的head：{}".format(header))
        logger.info("回复Client心跳消息的body：{}".format(body_pack))
        headPack = struct.pack(fmt_head, *header)
        data = headPack + body_pack
        return data
    
    def get_conf(self, site_id, queue_recv, conf_info):
        '''
        @:向ConfigServer查询Client的配置信息
        @:查询不成功查3次
        '''
        logger.info('执行get_conf,向ConfigServer发送查询信息')
        for i in range(Constant.try_times):
            logger.info('第{}次发送查询信息'.format(i+1))
            config_server.send_msg(site_id)
            key = str(site_id)
            res = queue_recv.get(key)
            if res:
                body_unpack = conf_info.get(key)
                conf_info.pop(key)
                return body_unpack
            else:
                logger.info('延时1秒再发送查询请求')
                time.sleep(1)
        else:
            logger.error('查询3次未查到{}的配置信息'.format(site_id))
        
    def handle_hb(self, head_unpack, body, conn, queue_recv, conf_info):
        """
        @:处理Client心跳消息
        @:心跳消息内容字段：
        site_id    client_disk_total    client_disk_free    config_version
        client_version    transactions_num    timestamp
        """
        logger.info('执行handle_hb，处理Client心跳消息')
        try:
            fmt = '!IQQHHII'
            body_unpack = struct.unpack(fmt, body)
            logger.info("收到Client心跳消息的body:{}".format(body_unpack))
        except:
            logger.error('Client心跳的消息体解析出错。')
        else:
            (site_id, client_disk_total, client_disk_free, config_version,
             client_version, transactions_num, timestamp) = body_unpack
          
            client_status = ClientStatus(site_id=site_id,
                                         client_disk_total=client_disk_total,
                                         client_disk_free=client_disk_free,
                                         config_version=config_version,
                                         client_version=client_version,
                                         transactions_num=transactions_num,
                                         timestamp=timestamp)
            with session_scope() as session:
                session.add(client_status)
         
            response = self._generate_resp_hb(head_unpack, site_id, queue_recv,
                                              conf_info)
#             conn.sendall(response)
            t = threading.Thread(target=conn.sendall, args=(response,))
            t.start()
    
    def select_addr(self, sgw_info, lock):
        """
        @:选择一个存储网关地址
        @:保存sgw信息的数据结构:
        {group_id1: [disk_free, [[addr1, addr2, ...], region_id, system_id, group_id1]],
        group_id2: [disk_free, [[addr3, addr4, ...], region_id, system_id, group_id2]],
        ...}
        addr = [ip, port, sgw_id]
        """
        logger.info('选择一个存储网关地址')
        lock.acquire()
        try:
            values = sgw_info.values()
            sgw_disk_free = []
            sgw_disk_temp = {}    
            for item in values:
                sgw_disk_free.append(item[0])
                sgw_disk_temp[item[0]] = item[1]
            sgw_disk_free.sort(reverse=True)        # 按降序排列后，选取可用磁盘容量最大的值
            max_disk_free = sgw_disk_free[0]
            g = (addr for addr in cycle(sgw_disk_temp[max_disk_free][0]))
            region_id = sgw_disk_temp[max_disk_free][1]
            system_id = sgw_disk_temp[max_disk_free][2]
            group_id = sgw_disk_temp[max_disk_free][3]
            return g, region_id, system_id, group_id
        finally:
            lock.release()
        
    def handle_upload(self, head_unpack, body, conn, sgw_info, lock):
        """
        @:处理Client转存路径请求
        (operation, region_id, site_id, app_id, timestamp, sgw_port,
         proxy_port, sgw_ip, proxy_ip, sgw_id, proxy_id, file_len,
         file_md5, file_name, metadata_len) = body_unpack
        """
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_upload,处理Client转存请求')
        total_size = head_unpack[0]
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
        
        fmt = Constant.FMT_TASKINFO_FIXED
        body_unpack = struct.unpack(fmt, body[: Constant.TASKINFO_FIXED_LENGTH])
        metadata_pack = body[Constant.TASKINFO_FIXED_LENGTH:]
        metadata_unpack = json.loads(metadata_pack.decode('utf-8'))
#         (operation, region_id, site_id, app_id, timestamp, sgw_port,
#          proxy_port, sgw_ip, proxy_ip, sgw_id, proxy_id, file_len,
#          file_md5, file_name, metadata_len) = body_unpack
        operation = body_unpack[0]
        site_id = body_unpack[2]
        app_id = body_unpack[3]
        timestamp = body_unpack[4]
        file_len = body_unpack[11]
        file_md5 = body_unpack[12]
        file_name = body_unpack[13]
        metadata_len = body_unpack[14]
        
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        user_id = metadata_unpack.get('user_id')
        customer_id = metadata_unpack.get('customer_id')
        command = Constant.CLIENT_UPLOAD_ROUTE_RESP
        
        values = sgw_info.values()
        if not sgw_info or not values:
            logger.error('sgw还没有发心跳消息注册')
            ack_code = Constant.ACK_CLIENT_UPLOAD_ROUTE_NOTFOUND 
        else:
            ack_code = Constant.ACK_CLIENT_UPLOAD_ROUTE
            
        g, region_id, system_id, group_id = self.select_addr(sgw_info, lock)
        addr = next(g)
        sgw_ip = proxy_ip = addr[0]
        sgw_port = proxy_port = addr[1]
        sgw_id = proxy_id = addr[2]
        logger.info("处理Client转存请求,选择的地址为：{}".format(addr))
        
        # 构造响应消息头
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, src_id, dst_id, trans_id, sequence, command,
                  ack_code, total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        
        # 构造消息体
        fmt_body = Constant.FMT_TASKINFO_FIXED
        body_resp = [operation, region_id, site_id, app_id, timestamp, sgw_port,
                     proxy_port, sgw_ip, proxy_ip, sgw_id, proxy_id, file_len,
                     file_md5, file_name, metadata_len]
        body_pack = struct.pack(fmt_body, *body_resp)
        logger.info("处理Client转存请求，回复的head为：{}".format(header))
        logger.info("处理Client转存请求，回复的body为：{}".format(body_resp))
        logger.info("处理Client转存请求，回复的metadata为：{}".format(metadata_unpack))
        data = head_pack + body_pack + metadata_pack
        conn.sendall(data)
        
#         metadata_internal = (site_id, app_id, file_name, region_id, system_id,
#                              group_id, user_id, customer_id, timestamp)
        metadata_info = MetadataInfo(site_id=site_id,
                                     app_id=app_id,
                                     file_name=file_name,
                                     region_id=region_id,
                                     system_id=system_id,
                                     group_id=group_id,
                                     user_id=user_id,
                                     customer_id=customer_id,
                                     timestamp=timestamp)
        with session_scope() as session:
            session.add(metadata_info)
        
    def handle_upload_success(self, body, metadata_info):
        """
        @:在处理上传请求时，由于还没有确认文件是否上传成功，那时还没有将元数据写入数据库，构造出以file_md5为键，
        @:以要存入数据库的的元数据信息为值的全局域字典。在获得上传成功的命令字后，再写入数据库。
        """
        fmt = Constant.FMT_TASKINFO_FIXED
        bodypack = struct.unpack(fmt, body[: Constant.TASKINFO_FIXED_LENGTH])
        file_md5 = bodypack[10]
        try:
            metadata_internal = metadata_info.get(file_md5)
        except:
            logger.error('上报文件上传成功请求task_info中的文件md5值与上传时的md5值不一样')
        else:
            metadata_sql = MetadataInfo(site_id=metadata_internal[0],
                                        app_id=metadata_internal[1],
                                        file_name=metadata_internal[2],
                                        region_id=metadata_internal[3],
                                        system_id=metadata_internal[4],
                                        group_id=metadata_internal[5],
                                        user_id=metadata_internal[6],
                                        customer_id=metadata_internal[7],
                                        timestamp=metadata_internal[8])
            with session_scope() as session:
                session.add(metadata_sql)
            
            metadata_info.pop(file_md5)
            
    def handle_query_num(self, head_unpack, body, conn, queue_recv, conf_info):
        """
        query_json用来描述用户的查询条件，格式为：
        {"site_id": [id1, id2, ...],
        "app_id": [id1, id2, ...],
        "user_id": [id1, id2, ...],
        "customer_id": [id1, id2, ...],
        "timestamp": [start_time, end_time],
        "order_by": [keyword1, keyword2…],
        "desc": bool}
        bodyPack: site_id只对应历史 的region_id，不含当前的region_id
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
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_query_num,处理Client查询记录数量的请求')
        total_size = head_unpack[0]
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        offset = head_unpack[12]
        count = head_unpack[13]
        
        local_res = self._get_local_query_num(body)
        logger.info("在本地查询到的记录数量为：{}".format(local_res))
        
        site_id = client_src_id
        query_body_unpack = self.get_conf(site_id, queue_recv, conf_info)
        logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
        remote_res = 0
        if query_body_unpack:
            site_region_id = query_body_unpack.get(str(site_id))
            if not site_region_id:
                for region_id in site_region_id:
                    remote_metadata_info = query_body_unpack.get(region_id)
                    meta_ip = str(remote_metadata_info[1])
                    meta_port = int(remote_metadata_info[2])
                    meta_src_id = remote_metadata_info[3]
                    addr = (meta_ip, meta_port)
                    sock = self._generate_meta_sock(addr)
                    self._send_query_num(head_unpack, body, sock, meta_src_id)
                    num = self._parse_remote_query_num(sock)
                    remote_res += num
        else:
            logger.error('经过{}次查询未查询到配置信息'.format(Constant.try_times))
        total = remote_res + local_res
        logger.info("在远端查询到的记录数量为：{}".format(remote_res))
        logger.info("查询到的记录数量总共为：{}".format(total))
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        command = Constant.CLIENT_QUERY_NUM_RESP
        ack_code = Constant.ACK_CLIENT_QUERY_NUM
        
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, src_id, dst_id, trans_id, sequence, command,
                  ack_code, total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        logger.info("处理Client查询记录数量的请求，回复的head为：{}".format(header))
        data = head_pack + body
        conn.sendall(data)
        
    def handle_query_data(self, head_unpack, body, conn, sgw_info, lock,
                          queue_recv, conf_info):
        """
        @:查询元数据信息
        """
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_query_data,处理Client查询元数据信息的请求')
        client_src_id = head_unpack[5]
        offset = head_unpack[12]
        count = head_unpack[13]
        site_id = client_src_id
        
        body_unpack = json.loads(body.decode('utf-8'))
        order_by = body_unpack.get('order_by')
        desc_key = body_unpack.get('desc')
        
        query_body_unpack = self.get_conf(site_id, queue_recv, conf_info)
        logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
        if query_body_unpack:
            site_region_id = query_body_unpack.get(str(site_id))
            if not site_region_id:
                local_ret = self._get_local_query_data(head_unpack, body,
                                                       sgw_info, lock)
                for region_id in site_region_id:
                    remote_metadata_info = query_body_unpack.get(region_id)
                    meta_ip = str(remote_metadata_info[1])
                    meta_port = int(remote_metadata_info[2])
                    meta_src_id = remote_metadata_info[3]
                    addr = (meta_ip, meta_port)
                    sock = self._generate_meta_sock(addr)
                    self._send_query_data(head_unpack, body, sock, meta_src_id)
                    meta_body = self.recv_remote_msg(sock)[1]
                    meta_body_unpack = json.loads(meta_body.decode('utf-8'))
                    remote_ret = meta_body_unpack.get('site_id')
                    local_ret.extend(remote_ret)
                local_tmp = sorted(local_ret, key=itemgetter(*order_by),
                                   reverse=desc_key)
                rets = local_tmp[offset: offset + count]
                self.proxy_query_data(head_unpack, rets, conn, sgw_info, lock)
            else:
                self.handle_local_query_data(head_unpack, body, conn, sgw_info,
                                            lock)
        else:
            self.handle_local_query_data(head_unpack, body, conn, sgw_info, lock)
            
    def proxy_query_data(self, head_unpack, rets, conn, sgw_info, lock):
        """
        @:代理查询功能
        """
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]

        g = self.select_addr(sgw_info, lock)[0]
        addr = next(g)
        sgw_ip = addr[0]
        sgw_port = addr[1]
        sgw_id = addr[2]
        
        fmt_body = Constant.FMT_TASKINFO_SEND
        body_tmp = []
        for ret in rets:
            site_id_tmp = ret.get('site_id')
            app_id_tmp = ret.get('app_id')
            file_name_tmp = ret.get('file_name')
            region_id_tmp = ret.get('region_id')
            user_id_tmp = ret.get('user_id')
            customer_id_tmp = ret.get('customer_id')
            timestamp_tmp = ret.get('timestamp')
            sgw_ip_tmp = ret.get('sgw_ip')
            sgw_port_tmp = ret.get('sgw_port')
            sgw_id_tmp = ret.get('sgw_id')
            proxy_ip_tmp = sgw_ip
            proxy_port_tmp = sgw_port
            proxy_id_tmp = sgw_id
            file_name = file_name_tmp.encode('utf-8')
            
            metadata = {}
            metadata['user_id'] = user_id_tmp
            metadata['customer_id'] = customer_id_tmp
            metadata_json = json.dumps(metadata)
            metadata_pack = metadata_json.encode('utf-8')
            metadata_len = len(metadata_pack)
            
            body_proxy = [region_id_tmp, site_id_tmp, app_id_tmp, timestamp_tmp,
                          sgw_port_tmp, proxy_port_tmp, sgw_ip_tmp,
                          proxy_ip_tmp, sgw_id_tmp, proxy_id_tmp, file_name,
                          metadata_len]
            body_pack = struct.pack(fmt_body, *body_proxy)
            body_tmp.append(body_pack + metadata_pack)
        body_query = b''.join(body_tmp)
        body_size = len(body_query)
        
        total_size = Constant.HEAD_LENGTH + body_size
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        command = Constant.CLIENT_QUERY_DATA_RESP
        ack_code = Constant.ACK_CLIENT_QUERY_DATA
         
        # 构造消息头部
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, src_id, dst_id, trans_id, sequence, command,
                  ack_code, total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        logger.info("处理Client查询元数据信息的请求，回复的head为：{}".format(header))
        data = head_pack + body_query
        conn.sendall(data)
            
#     def handle_query_data(self, head_unpack, body, conn, sgw_info, lock,
#                           queue_recv, conf_info):
#         """
#         :查询元数据信息
#         """
# #         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
# #          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
#         client_src_id = head_unpack[5]
#         offset = head_unpack[12]
#         count = head_unpack[13]
#         site_id = client_src_id
#         
#         bodyPack = json.loads(body.decode('utf-8'))
# #         site_id = bodyPack.get('site_id')[0]
# #         app_id = bodyPack.get('app_id')
# #         user_id = bodyPack.get('user_id')
# #         customer_id = bodyPack.get('customer_id')
# #         timestamp = bodyPack.get('timestamp')
# #         start = timestamp[0]
# #         end = timestamp[1]
# #         order_by = bodyPack.get('order_by')
# #         desc_key = bodyPack.get('desc')
#         
#         query_bodyPack = self.get_conf(site_id, queue_recv, conf_info)
#         if query_bodyPack:
#             site_region_id = query_bodyPack.get(str(site_id))
#             if not site_region_id:
#                 offset_local = 0
#                 local_ret = self._get_local_query_data(body, sgw_info, lock,
#                                                        offset_local, count)
#                 remote_dict = {}
#                 remote_dict['region_id'] = local_ret
#                 for region_id in site_region_id:
#                     remote_metadata_info = query_bodyPack.get(region_id)
#                     meta_ip = str(remote_metadata_info[1])
#                     meta_port = int(remote_metadata_info[2])
#                     meta_src_id = remote_metadata_info[3]
#                     addr = (meta_ip, meta_port)
#                     sock = self._generate_meta_sock(addr)
#                     offset_remote = 0
#                     self._send_query_data(head_unpack, body, sock, meta_src_id,
#                                           offset_remote, count)
#                     meta_body = self.recv_remote_msg(sock)[1]
#                     meta_bodypack = meta_body.decode('utf-8')
#                     remote_ret = meta_bodypack.get('site_id')
#                     remote_dict[region_id] = remote_ret
#                     local_ret.extend(remote_ret)
#                 order_by = bodyPack.get('order_by')
#                 desc_key = bodyPack.get('desc')
#                 local_tmp = sorted(local_ret, key=itemgetter(*order_by),
#                                    reverse=desc_key)
#                 ret_tmp = local_tmp[: count]
#                 ret_tmp_set = set(ret_tmp)
#                 local_region_id = remote_dict.get('region_id')
#                 local_region_id_set = set(local_region_id)
#                 local_intersection_set = local_region_id_set.intersection(ret_tmp_set)
#                 local_intersection_list = list(local_intersection_set)
#                 local_num = len(local_intersection_list)
#                 local_region_id
#                 
#                 n = offset/count + 1
#                 for i in range(n):
#                     local_tmp[i*count: (i+1)*count]
#                     
#                 
# #                     self.proxy_query_data(head_unpack, body, meta_body, conn,
# #                                      sgw_info, lock)
#                 
# #                 self.proxy_query_data(head_unpack, body, local_ret, remote_ret, conn,
# #                                   sgw_info, lock)
#             else:
#                 self.handle_local_query_data(head_unpack, body, conn, sgw_info, lock)
#         else:
#             self.handle_local_query_data(head_unpack, body, conn, sgw_info, lock)
        
    def _generate_meta_sock(self, addr):
        """
        @:创建与RemoteMetadataServer通信的socket
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(addr)
        except OSError:
            logger.error('该请求的地址{}无效，与RemoteMetadataServer连接失败'.format(addr))
            sock.close()
        return sock
        
    def _generate_query_num(self, head_unpack, body, meta_src_id):
        """
        @:生成向RemoteMetadataServer查询记录数量的请求消息
        """
#         (total_size, major, minor, src_type, dst_type, src_id, client_dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        total_size = head_unpack[0]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        ack_code = head_unpack[10]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
    
        dst_type = Constant.METADATA_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = int(meta_src_id, 16)
        command = Constant.REMOTE_QUERY_NUM
        
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code,
                  total, offset, count]
        head_pack = struct.pack(Constant.FMT_COMMON_HEAD, *header)
        data = head_pack + body
        return data
    
    def _send_query_num(self, head_unpack, body, sock, meta_src_id):
        """
        @:向RemoteMetadataServer发送查询消息
        """
        data = self._generate_query_num(head_unpack, body, meta_src_id)
        sock.sendall(data)
        
    def _generate_query_data(self, headpack, body, meta_src_id):
        """
        @:生成向RemoteMetadataServer查询的请求消息
        """
#         (total_size, major, minor, src_type, dst_type, src_id, client_dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = headpack
        total_size = headpack[0]
        trans_id = headpack[7]
        sequence = headpack[8]
        ack_code = headpack[10]
        total = headpack[11]
        offset = headpack[12]
        count = headpack[13]
         
        dst_type = Constant.METADATA_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = int(meta_src_id, 16)
        command = Constant.REMOTE_QUERY_DATA
        
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code,
                  total, offset, count]
        headPack = struct.pack(Constant.FMT_COMMON_HEAD, *header)
        data = headPack + body
        return data
    
    def _send_query_data(self, head_unpack, body, sock, meta_src_id):
        """
        @:向RemoteMetadataServer发送查询消息
        """
#         (total_size, major, minor, src_type, dst_type, src_id, client_dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        total_size = head_unpack[0]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        ack_code = head_unpack[10]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
         
        dst_type = Constant.METADATA_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = int(meta_src_id, 16)
        command = Constant.REMOTE_QUERY_DATA
        
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code,
                  total, offset, count]
        head_pack = struct.pack(Constant.FMT_COMMON_HEAD, *header)
        data = head_pack + body
        sock.sendall(data)
        
#     def recv_remote_msg(self, sock):
#         """
#         :接收RemoteMetadataServer发回的消息
#         """
#         data = b''
#         while True:
#             more = sock.recv(Constant.BUFSIZE)
#             if not more:
#                 break
#             data += more
#             while True:
#                 if len(data) < Constant.HEAD_LENGTH:
#                     logger.warning('接收到的数据包长度为{}，小于消息头部长度64，'
#                                    '继续接收数据'.format(len(data)))
#                     break
#                 head_unpack = struct.unpack(Constant.FMT_COMMON_HEAD,
#                                          data[:Constant.HEAD_LENGTH])
#                 total_size = head_unpack[0]
#                 if len(data) < total_size:
#                     logger.warning('接收到的数据包长度为{}，总共为{}，数据包不完整，'
#                                    '继续接收数据'.format(len(data), total_size))
#                     break
#                 body = data[Constant.HEAD_LENGTH: total_size]
#                 return head_unpack, body
        
    def recvall_remote(self, sock, length):
        """
        @:接收RemoteMetadataServer发回的消息
        """
        blocks = []
        while length:
            block = sock.recv(length)
            if not block:
                raise EOFError
            length -= len(block)
            blocks.append(block)
        return b''.join(blocks)
     
    def recv_remote_msg(self, sock):
        """
        @:解析RemoteMetadataServer发回的消息
        """
        header = self.recvall_remote(sock, Constant.HEAD_LENGTH)
        fmt_head = Constant.FMT_COMMON_HEAD
        head_unpack = struct.unpack(fmt_head, header)
        total_size = head_unpack[0]
        body_size = total_size - Constant.HEAD_LENGTH
        body = self.recvall_remote(sock, body_size)
        return head_unpack, body
            
    def _parse_remote_query_num(self, sock):
        """
        @:解析RemoteMetadataServer返回查询到的记录条数
        """
        headpack = self.recv_remote_msg(sock)[0]
        total = headpack[11]
        return total
    
#     def proxy_query_data(self, headpack, body, meta_body, conn, sgw_info, lock):
#         """
#         :代理转发功能
#         """
#         bodyPack = json.loads(body.decode('utf-8'))
#         user_id = bodyPack.get('user_id')[0]
#         customer_id = bodyPack.get('customer_id')[0]
#         
#         metadata = {}
#         metadata['user_id'] = user_id
#         metadata['customer_id'] = customer_id
#         metadata_json = json.dumps(metadata)
#         metadata_pack = metadata_json.encode('utf-8')
#         metadata_len = len(metadata_pack)
#         
# #         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
# #          trans_id, sequence, command, ack_code, total, offset, count) = headpack
#         client_src_id = headpack[5]
#         trans_id = headpack[7]
#         sequence = headpack[8]
#         total = headpack[11]
#         offset = headpack[12]
#         count = headpack[13]
#          
# #         (meta_total_size, major, minor, meta_src_type, meta_dst_type,
# #          meta_src_id, meta_dst_id, trans_id, sequence, meta_command,
# #          meta_ack_code, total, offset, count) = meta_headpack
#          
#         task_info_size = Constant.TASKINFO_FIXED_LENGTH + metadata_len
#         total_size = (Constant.HEAD_LENGTH + task_info_size * count)
#         src_id = int(Config.src_id, 16)
#         dst_id = client_src_id
#         command = Constant.CLIENT_QUERY_DATA_RESP
#         ack_code = Constant.ACK_CLIENT_QUERY_DATA
#         
#         fmt_head = Constant.FMT_COMMON_HEAD
#         header = [total_size, self.major, self.minor, self.src_type,
#                   self.dst_type, src_id, dst_id, trans_id, sequence, command,
#                   ack_code, total, offset, count]
#         headPack = struct.pack(fmt_head, *header)
#         conn.sendall(headPack)
#         
#         g = self.select_addr(sgw_info, lock)[0]
#         addr = next(g)
#         local_sgw_ip = addr[0]
#         local_sgw_port = addr[1]
#         local_sgw_id = addr[2]
#         
#         n = task_info_size
#         fmt_body = '!2xHIIIHH4I8x33x512sH{}s'.format(metadata_len)
#         for i in count:
#             metabody_tmp = meta_body[i*n : (i+1)*n]
#             metabody_pack_tmp = struct.unpack(fmt_body, metabody_tmp)
#             metabody_pack = metabody_pack_tmp[:-1]
# #             (operation, region_id, site_id, app_id, timestamp, sgw_port,
# #              proxy_port, sgw_ip, proxy_ip, file_len, file_md5, file_name,
# #              metadata_len) = metabody_pack[:-1]
#             region_id = metabody_pack[1]
#             site_id = metabody_pack[2]
#             app_id = metabody_pack[3]
#             timestamp = metabody_pack[4]
#             sgw_port = metabody_pack[5]
#             sgw_ip = metabody_pack[7]
#             sgw_id = metabody_pack[9]
#             file_name = metabody_pack[13]
#             metadata_len = metabody_pack[14]
#             
#             proxy_ip = local_sgw_ip
#             proxy_port = local_sgw_port
#             proxy_id = local_sgw_id
#             fmt_resp = Constant.FMT_TASKINFO_SEND
#             resp_body = [region_id, site_id, app_id, timestamp, sgw_port,
#                          proxy_port, sgw_ip, proxy_ip, sgw_id, proxy_id,
#                          file_name, metadata_len]
#             resp_body_pack = struct.pack(fmt_resp, *resp_body)
#             conn.sendall(resp_body_pack + metadata_pack)
            
    def _parse_remote_del_msg(self, sock):
        """
        @:解析RemoteMetadataServer发送的删除消息
        """
        head_unpack = self.recv_remote_msg(sock)[0]
        ack_code = head_unpack[10]
        if ack_code == Constant.ACK_REMOTE_DEL_SUCCESS:
            flag = True
        elif ack_code == Constant.ACK_REMOTE_DEL_FAILED:
            flag = False
        else:
            logger.error('RemoteMetadataServer使用了未定义的删除响应码')
        return flag
        
    def _get_local_query_num(self, body):
        """
        @:MetadataServer收到远端查询请求后只进行本地查询
        {"site_id": [id1, id2, ...],
        "app_id": [id1, id2, ...],
        "user_id": [id1, id2, ...],
        "customer_id": [id1, id2, ...],
        "timestamp": [start_time, end_time],
        "order_by": [keyword1, keyword2…],
        "desc": bool}
        """
        body_unpack = json.loads(body.decode('utf-8'))
        logger.info(body_unpack)
        site_id = body_unpack.get('site_id')[0]
        app_id = body_unpack.get('app_id')
        user_id = body_unpack.get('user_id')
        customer_id = body_unpack.get('customer_id')
        timestamp = body_unpack.get('timestamp')
        start = timestamp[0]
        end = timestamp[1]
        
        # 只查询数量 ，不用排序
        with session_scope() as session:
            query = session.query(MetadataInfo)
            filt_ret = query.filter(and_(MetadataInfo.site_id == site_id,
                                     MetadataInfo.app_id.in_(app_id),
                                     MetadataInfo.user_id.in_(user_id),
                                     MetadataInfo.customer_id.in_(customer_id),
                                     MetadataInfo.timestamp.between(start, end)))
            query_nums = filt_ret.count()
        return query_nums
    
    def handle_remote_query_num(self, head_unpack, body, conn):
        """
        @:将本地查询到的总记录条数发给发起请求的MetadataServer
        """
#         (total_size, major, minor, src_type, dst_type, meta_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_remote_query_num,处理远端MetadataServer'
                    '查询元数据记录数量的请求')
        total_size = head_unpack[0]
        meta_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        offset = head_unpack[12]
         
        dst_type = Constant.METADATA_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = meta_src_id
        command = Constant.REMOTE_QUERY_NUM_RESP
        ack_code = Constant.ACK_REMOTE_QUERY_NUM
        
        total = self._get_local_query_num(body)
        count = 0
        
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code,
                  total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        data = head_pack + body
        conn.sendall(data)
        
    def _get_local_query_data(self, head_unpack, body, sgw_info, lock):
        """
        @:MetadataServer收到查询请求后进行本地查询
        """
        offset = head_unpack[12]
        count = head_unpack[13]
        
        body_unpack = json.loads(body.decode('utf-8'))
        site_id = body_unpack.get('site_id')[0]
        app_id = body_unpack.get('app_id')
        user_id = body_unpack.get('user_id')
        customer_id = body_unpack.get('customer_id')
        timestamp = body_unpack.get('timestamp')
        start = timestamp[0]
        end = timestamp[1]
        order_by = body_unpack.get('order_by')
        desc_key = body_unpack.get('desc')
        
        g = self.select_addr(sgw_info, lock)[0]
        addr = next(g)
        sgw_ip = proxy_ip = addr[0]
        sgw_port = proxy_port = addr[1]
        sgw_id = proxy_id = addr[2]
        
        local_metadata = []
        with session_scope() as session:
            query = session.query(MetadataInfo)
            filt_ret = query.filter(and_(MetadataInfo.site_id == site_id,
                                     MetadataInfo.app_id.in_(app_id),
                                     MetadataInfo.user_id.in_(user_id),
                                     MetadataInfo.customer_id.in_(customer_id),
                                     MetadataInfo.timestamp.between(start, end)))
            if not desc_key:
                for key in order_by:
                    ret = filt_ret.order_by(asc(key)).all()
            else:
                for key in order_by:
                    ret = filt_ret.order_by(desc(key)).all()
            
            for metadata_info in ret[: offset + count]:
                site_id_tmp = metadata_info.site_id
                app_id_tmp = metadata_info.app_id
                file_name = metadata_info.file_name
                region_id = metadata_info.region_id
                user_id_tmp = metadata_info.user_id
                customer_id_tmp = metadata_info.customer_id
                timestamp_tmp = metadata_info.timestamp
                
                tmp_dict = {}
                tmp_dict['site_id'] = site_id_tmp
                tmp_dict['app_id'] = app_id_tmp
                tmp_dict['file_name'] = file_name
                tmp_dict['region_id'] = region_id
                tmp_dict['user_id'] = user_id_tmp
                tmp_dict['customer_id'] = customer_id_tmp
                tmp_dict['timestamp'] = timestamp_tmp
                tmp_dict['sgw_ip'] = sgw_ip
                tmp_dict['proxy_ip'] = proxy_ip
                tmp_dict['sgw_port'] = sgw_port
                tmp_dict['proxy_port'] = proxy_port
                tmp_dict['sgw_id'] = sgw_id
                tmp_dict['proxy_id'] = proxy_id
                local_metadata.append(tmp_dict)
        return local_metadata
    
#     def _get_local_query_data(self, body, sgw_info, lock, offset, count):
#         """
#         :MetadataServer收到查询请求后进行本地查询
#         """
#         body_unpack = json.loads(body.decode('utf-8'))
#         site_id = body_unpack.get('site_id')[0]
#         app_id = body_unpack.get('app_id')
#         user_id = body_unpack.get('user_id')
#         customer_id = body_unpack.get('customer_id')
#         timestamp = body_unpack.get('timestamp')
#         start = timestamp[0]
#         end = timestamp[1]
#         order_by = body_unpack.get('order_by')
#         desc_key = body_unpack.get('desc')
#         
#         g = self.select_addr(sgw_info, lock)[0]
#         addr = next(g)
#         sgw_ip = proxy_ip = addr[0]
#         sgw_port = proxy_port = addr[1]
#         sgw_id = proxy_id = addr[2]
#         
#         local_metadata = []
#         with session_scope() as session:
#             query = session.query(MetadataInfo)
#             filt_ret = query.filter(and_(MetadataInfo.site_id == site_id,
#                                      MetadataInfo.app_id.in_(app_id),
#                                      MetadataInfo.user_id.in_(user_id),
#                                      MetadataInfo.customer_id.in_(customer_id),
#                                      MetadataInfo.timestamp.between(start, end)))
#             if not desc_key:
#                 for key in order_by:
#                     ret = filt_ret.order_by(asc(key)).all()
#             else:
#                 for key in order_by:
#                     ret = filt_ret.order_by(desc(key)).all()
#             
#             for metadata_info in ret[offset: offset + count]:
#                 site_id_tmp = metadata_info.site_id
#                 app_id_tmp = metadata_info.app_id
#                 file_name = metadata_info.file_name
#                 region_id = metadata_info.region_id
#                 user_id_tmp = metadata_info.user_id
#                 customer_id_tmp = metadata_info.customer_id
#                 timestamp_tmp = metadata_info.timestamp
#                 
#                 tmp_dict = {}
#                 tmp_dict['site_id'] = site_id_tmp
#                 tmp_dict['app_id'] = app_id_tmp
#                 tmp_dict['file_name'] = file_name
#                 tmp_dict['region_id'] = region_id
#                 tmp_dict['user_id'] = user_id_tmp
#                 tmp_dict['customer_id'] = customer_id_tmp
#                 tmp_dict['timestamp'] = timestamp_tmp
#                 tmp_dict['sgw_ip'] = sgw_ip
#                 tmp_dict['proxy_ip'] = proxy_ip
#                 tmp_dict['sgw_port'] = sgw_port
#                 tmp_dict['proxy_port'] = proxy_port
#                 tmp_dict['sgw_id'] = sgw_id
#                 tmp_dict['proxy_id'] = proxy_id
#                 local_metadata.append(tmp_dict)
#         return local_metadata
    
    def handle_local_query_data(self, head_unpack, body, conn, sgw_info, lock):
        """
        :MetadataServer发送本地查询到的数据
        {"site_id": [id1, ...],
        "app_id": [id1, id2, ...],
        "user_id": [id1, id2, ...],
        "customer_id": [id1, id2, ...],
        "timestamp": [start_time, end_time],
        "order_by": [keyword1, keyword2, ...],
        "desc": bool}
        """
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_local_query_data,本地处理Client查询元数据信息的请求')
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
         
        g = self.select_addr(sgw_info, lock)[0]
        addr = next(g)
        sgw_ip = proxy_ip = addr[0]
        sgw_port = proxy_port = addr[1]
        sgw_id = proxy_id = addr[2]
         
        body_unpack = json.loads(body.decode('utf-8'))
        site_id = body_unpack.get('site_id')[0]
        app_id = body_unpack.get('app_id')
        user_id = body_unpack.get('user_id')
        customer_id = body_unpack.get('customer_id')
        timestamp = body_unpack.get('timestamp')
        start = timestamp[0]
        end = timestamp[1]
        order_by = body_unpack.get('order_by')
        desc_key = body_unpack.get('desc')
        
        fmt_body = Constant.FMT_TASKINFO_SEND
        body_tmp = []
        with session_scope() as session:
            query = session.query(MetadataInfo)
            filt_ret = query.filter(and_(MetadataInfo.site_id == site_id,
                                     MetadataInfo.app_id.in_(app_id),
                                     MetadataInfo.user_id.in_(user_id),
                                     MetadataInfo.customer_id.in_(customer_id),
                                     MetadataInfo.timestamp.between(start, end)))
            if not desc_key:
                for key in order_by:
                    ret = filt_ret.order_by(asc(key)).all()
            else:
                for key in order_by:
                    ret = filt_ret.order_by(desc(key)).all()
                
            for metadata_info in ret[offset: offset + count]:
                site_id_tmp = metadata_info.site_id
                app_id_tmp = metadata_info.app_id
                file_name_tmp = metadata_info.file_name
                region_id_tmp = metadata_info.region_id
                user_id_tmp = metadata_info.user_id
                customer_id_tmp = metadata_info.customer_id
                timestamp_tmp = metadata_info.timestamp
                file_name = file_name_tmp.encode('utf-8')
                
                metadata = {}
                metadata['user_id'] = user_id_tmp
                metadata['customer_id'] = customer_id_tmp
                metadata_json = json.dumps(metadata)
                metadata_pack = metadata_json.encode('utf-8')
                metadata_len = len(metadata_pack)
                
                body_resp = [region_id_tmp, site_id_tmp, app_id_tmp, timestamp_tmp,
                             sgw_port, proxy_port, sgw_ip, proxy_ip, sgw_id,
                             proxy_id, file_name, metadata_len]
                body_pack = struct.pack(fmt_body, *body_resp)
                body_tmp.append(body_pack + metadata_pack)
        body_query = b''.join(body_tmp)
        body_size = len(body_query)
        
        total_size = Constant.HEAD_LENGTH + body_size
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        command = Constant.CLIENT_QUERY_DATA_RESP
        ack_code = Constant.ACK_CLIENT_QUERY_DATA
         
        # 构造消息头部
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, src_id, dst_id, trans_id, sequence, command,
                  ack_code, total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        logger.info("处理Client查询元数据信息的请求，回复的head为：{}".format(header))
        data = head_pack + body_query
        conn.sendall(data)
        
    def handle_remote_query_data(self, head_unpack, body, conn, sgw_info, lock):
        """
        @:处理远端查询到的数据
        """
#         (total_size, major, minor, src_type, dst_type, meta_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_remote_query_data,处理远端MetadataServer'
                    '查询元数据信息的请求')
        meta_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
        
        g = self.select_addr(sgw_info, lock)[0]
        addr = next(g)
        sgw_ip = proxy_ip = addr[0]
        sgw_port = proxy_port = addr[1]
        sgw_id = proxy_id = addr[2]
        
        body_unpack = json.loads(body.decode('utf-8'))
        site_id = body_unpack.get('site_id')[0]
        app_id = body_unpack.get('app_id')
        user_id = body_unpack.get('user_id')
        customer_id = body_unpack.get('customer_id')
        timestamp = body_unpack.get('timestamp')
        start = timestamp[0]
        end = timestamp[1]
        order_by = body_unpack.get('order_by')
        desc_key = body_unpack.get('desc')
        
        remote_metadata = []
        with session_scope() as session:
            query = session.query(MetadataInfo)
            filt_ret = query.filter(and_(MetadataInfo.site_id == site_id,
                                     MetadataInfo.app_id.in_(app_id),
                                     MetadataInfo.user_id.in_(user_id),
                                     MetadataInfo.customer_id.in_(customer_id),
                                     MetadataInfo.timestamp.between(start, end)))
            if not desc_key:
                for key in order_by:
                    ret = filt_ret.order_by(asc(key)).all()
            else:
                for key in order_by:
                    ret = filt_ret.order_by(desc(key)).all()
                
            for metadata_info in ret[: offset + count]:
                site_id_tmp = metadata_info.site_id
                app_id_tmp = metadata_info.app_id
                file_name = metadata_info.file_name
                region_id = metadata_info.region_id
                user_id_tmp = metadata_info.user_id
                customer_id_tmp = metadata_info.customer_id
                timestamp_tmp = metadata_info.timestamp
                
                tmp_dict = {}
                tmp_dict['site_id'] = site_id_tmp
                tmp_dict['app_id'] = app_id_tmp
                tmp_dict['file_name'] = file_name
                tmp_dict['region_id'] = region_id
                tmp_dict['user_id'] = user_id_tmp
                tmp_dict['customer_id'] = customer_id_tmp
                tmp_dict['timestamp'] = timestamp_tmp
                tmp_dict['sgw_ip'] = sgw_ip
                tmp_dict['proxy_ip'] = proxy_ip
                tmp_dict['sgw_port'] = sgw_port
                tmp_dict['proxy_port'] = proxy_port
                tmp_dict['sgw_id'] = sgw_id
                tmp_dict['proxy_id'] = proxy_id
                remote_metadata.append(tmp_dict)
        
        body_tmp = {}
        body_tmp['site_id'] = remote_metadata
        body_json = json.dumps(body_tmp)
        body_pack = body_json.encode('utf-8')
        body_size = len(body_pack)
        
        total_size = Constant.HEAD_LENGTH + body_size
        dst_type = Constant.METADATA_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = meta_src_id
        command = Constant.REMOTE_QUERY_DATA_RESP
        ack_code = Constant.ACK_REMOTE_QUERY_DATA
        
        # 构造消息头部
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code, total,
                  offset, count]
        head_pack = struct.pack(fmt_head, *header)
        
        data = head_pack + body_pack
        conn.sendall(data)
                
#     def handle_remote_query_data(self, head_unpack, body, conn, sgw_info, lock):
#         """
#         :处理远端查询到的数据
#         """
# #         (total_size, major, minor, src_type, dst_type, meta_src_id, dst_id,
# #          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
#         meta_src_id = head_unpack[5]
#         trans_id = head_unpack[7]
#         sequence = head_unpack[8]
#         total = head_unpack[11]
#         offset = head_unpack[12]
#         count = head_unpack[13]
#         
#         body_unpack = json.loads(body.decode('utf-8'))
#         user_id = body_unpack.get('user_id')[0]
#         customer_id = body_unpack.get('customer_id')[0]
#         
#         metadata = {}
#         metadata['user_id'] = user_id
#         metadata['customer_id'] = customer_id
#         metadata_json = json.dumps(metadata)
#         metadata_pack = metadata_json.encode('utf-8')
#         metadata_len = len(metadata_pack)
#         
#         total_size = (Constant.HEAD_LENGTH +
#                       (Constant.TASKINFO_FIXED_LENGTH + metadata_len) * count)
#         dst_type = Constant.METADATA_TYPE
#         src_id = int(Config.src_id, 16)
#         dst_id = meta_src_id
#         command = Constant.REMOTE_QUERY_DATA_RESP
#         ack_code = Constant.ACK_REMOTE_QUERY_DATA
#         
#         # 构造消息头部
#         fmt_head = Constant.FMT_COMMON_HEAD
#         header = [total_size, self.major, self.minor, self.src_type, dst_type,
#                   src_id, dst_id, trans_id, sequence, command, ack_code, total,
#                   offset, count]
#         head_pack = struct.pack(fmt_head, *header)
#         conn.sendall(head_pack)
#         
#         g = self.select_addr(sgw_info, lock)[0]
#         addr = next(g)
#         sgw_ip = proxy_ip = addr[0]
#         sgw_port = proxy_port = addr[1]
#         sgw_id = proxy_id = addr[2]
#         
#         # 发送消息体
#         site_id = body_unpack.get('site_id')[0]
#         app_id = body_unpack.get('app_id')
#         user_id = body_unpack.get('user_id')
#         customer_id = body_unpack.get('customer_id')
#         timestamp = body_unpack.get('timestamp')
#         start = timestamp[0]
#         end = timestamp[1]
#         order_by = body_unpack.get('order_by')
#         desc_key = body_unpack.get('desc')
#         
#         fmt_body = Constant.FMT_TASKINFO_SEND
#         body_tmp = []
#         with session_scope() as session:
#             query = session.query(MetadataInfo)
#             filt_ret = query.filter(and_(MetadataInfo.site_id == site_id,
#                                      MetadataInfo.app_id.in_(app_id),
#                                      MetadataInfo.user_id.in_(user_id),
#                                      MetadataInfo.customer_id.in_(customer_id),
#                                      MetadataInfo.timestamp.between(start, end)))
#             if not desc_key:
#                 for key in order_by:
#                     ret = filt_ret.order_by(asc(key)).all()
#             else:
#                 for key in order_by:
#                     ret = filt_ret.order_by(desc(key)).all()
#                 
#             for metadata_info in ret[offset: offset + count]:
#                 site_id = metadata_info.site_id
#                 app_id = metadata_info.app_id
#                 file_name_tmp = metadata_info.file_name
#                 region_id = metadata_info.region_id
#                 timestamp = metadata_info.timestamp
#                 file_name = file_name_tmp.encode('utf-8')
#                 
#                 body = [region_id, site_id, app_id, timestamp, sgw_port,
#                         proxy_port, sgw_ip, proxy_ip, sgw_id, proxy_id,
#                         file_name, metadata_len]
#                 body_pack = struct.pack(fmt_body, *body)
#                 body_tmp.append(body_pack + metadata_pack)
#         body_query = b''.join(body_tmp)
#         conn.sendall(body_query)
            
    def handle_delete(self, head_unpack, body, conn, sgw_info, lock, queue_recv,
                      conf_info):
        """
        @:处理Client的删除请求
        """
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_delete,处理Client删除元数据的请求')
        total_size = head_unpack[0]
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        ack_code = head_unpack[10]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
        
        fmt = Constant.FMT_TASKINFO_FIXED
        body_unpack = struct.unpack(fmt, body[: Constant.TASKINFO_FIXED_LENGTH])
        metadata_pack = body[Constant.TASKINFO_FIXED_LENGTH:]
        metadata_unpack = json.loads(metadata_pack.decode('utf-8'))
        
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        command = Constant.CLIENT_DEL_RESP
            
        flag_local, resp_body_pack = self.handle_local_del(body_unpack,
                                                metadata_unpack, sgw_info, lock)
        if flag_local:
            ack_code = Constant.ACK_CLIENT_DEL_SUCCESS
            
            fmt_head = Constant.FMT_COMMON_HEAD
            header = [total_size, self.major, self.minor, self.src_type,
                      self.dst_type, src_id, dst_id, trans_id, sequence, command,
                      ack_code, total, offset, count]
            head_pack = struct.pack(fmt_head, *header)
            logger.info("处理Client删除元数据的请求,回复的head为：{}".format(header))
            data = head_pack + resp_body_pack + metadata_pack
            conn.sendall(data)
        else:
            site_id = client_src_id
            query_body_unpack = self.get_conf(site_id, queue_recv, conf_info)
            logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
            site_region_id = query_body_unpack.get(str(site_id))
            if not site_region_id:
                i = 0
                while i < len(site_region_id):
                    region_id = site_region_id[i]
                    remote_metadata_info = query_body_unpack.get(region_id)
                    meta_ip = str(remote_metadata_info[1])
                    meta_port = int(remote_metadata_info[2])
                    meta_src_id = remote_metadata_info[3]
                    addr = (meta_ip, meta_port)
                    sock = self._generate_meta_sock(addr)
                    self._send_del_msg(head_unpack, body, sock, meta_src_id)
                    remote_body = self.recv_remote_msg(sock)[1] 
                    flag_remote = self._parse_remote_del_msg(sock)
                    if flag_remote:
                        self.proxy_del(head_unpack, remote_body, conn,
                                       sgw_info, lock)
                        break
                    else:
                        i += 1
            else:
                ack_code = Constant.ACK_CLIENT_DEL_FAILED
                fmt_head = Constant.FMT_COMMON_HEAD
                header = [total_size, self.major, self.minor, self.src_type,
                          self.dst_type, src_id, dst_id, trans_id, sequence,
                          command, ack_code, total, offset, count]
                head_pack = struct.pack(fmt_head, *header)
                logger.info("处理Client删除元数据的请求,回复的head为：{}".format(header))
                logger.info("处理Client删除元数据的请求,回复的body为：{}".format(body))
                data = head_pack + body
                conn.sendall(data)
                
    def proxy_del(self, head_unpack, remote_body, conn, sgw_info, lock):
        """
        @:代理删除
        """
        total_size = head_unpack[0]
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        ack_code = head_unpack[10]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
        
        fmt_body = Constant.FMT_TASKINFO_FIXED
        remote_body_unpack = struct.unpack(fmt_body,
                                remote_body[: Constant.TASKINFO_FIXED_LENGTH])
        metadata_pack = remote_body[Constant.TASKINFO_FIXED_LENGTH:]
        operation = remote_body_unpack[0]
        site_id = remote_body_unpack[2]
        app_id = remote_body_unpack[3]
        timestamp = remote_body_unpack[4]
        sgw_port = remote_body_unpack[5]
        sgw_ip = remote_body_unpack[7]
        sgw_id = remote_body[9]
        file_len = remote_body_unpack[11]
        file_md5 = remote_body_unpack[12]
        file_name = remote_body_unpack[13]
        metadata_len = remote_body_unpack[14]
        
        g, region_id = self.select_addr(sgw_info, lock)[0: 2]
        addr = next(g)
        proxy_ip = addr[0]
        proxy_port = addr[1]
        proxy_id = addr[2]
        body = [operation, region_id, site_id, app_id, timestamp, sgw_port,
                proxy_port, sgw_ip, proxy_ip, sgw_id, proxy_id, file_len,
                file_md5, file_name, metadata_len]
        body_pack = struct.pack(fmt_body, *body)
        
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        command = Constant.CLIENT_DEL_RESP
        
        if ack_code == Constant.ACK_REMOTE_DEL_SUCCESS:
            ack_code = Constant.ACK_CLIENT_DEL_SUCCESS
        else:
            ack_code = Constant.ACK_CLIENT_DEL_FAILED
            
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, src_id, dst_id, trans_id, sequence, command,
                  ack_code, total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        logger.info("处理Client删除元数据的请求,回复的head为：{}".format(header))
        logger.info("处理Client删除元数据的请求,回复的body为：{}".format(body))
        data = head_pack + body_pack + metadata_pack
        conn.sendall(data)
            
    def _generate_del_msg(self, head_unpack, body, meta_src_id):
        """
        @:生成向RemoteMetadataServer发起删除记录的请求消息
        """
#         (total_size, major, minor, src_type, dst_type, src_id, client_dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        total_size = head_unpack[0]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        ack_code = head_unpack[10]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
         
        dst_type = Constant.METADATA_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = int(meta_src_id, 16)
        command = Constant.REMOTE_DEL
        
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code,
                  total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        data = head_pack + body
        return data        
    
    def _send_del_msg(self, head_unpack, body, sock, meta_src_id):
        """
        @:向RemoteMetadataServer发送删除消息
        """
        data = self._generate_del_msg(head_unpack, body, meta_src_id)
        sock.sendall(data)
        
    def handle_local_del(self, body_unpack, metadata_unpack, sgw_info, lock):
        """
        @:MetadataServer收到删除请求后进行本地删除
        site_id    app_id    file_name    region_id    system_id    group_id
        user_id    customer_id    timestamp
        """
#         (operation, region_id, site_id, app_id, timestamp, sgw_port,
#          proxy_port, sgw_ip, proxy_ip, sgw_id, proxy_id, file_len,
#          file_md5, file_name, metadata_len) = body_unpack
        logger.info('执行handle_local_del,本地处理Client删除元数据的请求')
        operation = body_unpack[0]
        site_id = body_unpack[2]
        app_id = body_unpack[3]
        timestamp = body_unpack[4]
        file_len = body_unpack[11]
        file_md5 = body_unpack[12]
        file_name = body_unpack[13]
        metadata_len = body_unpack[14]
        
        user_id = metadata_unpack.get('user_id')
        customer_id = metadata_unpack.get('customer_id')
        
        with session_scope() as session:
            query = session.query(MetadataInfo)
            filt_ret = query.filter(and_(MetadataInfo.site_id == site_id,
                                    MetadataInfo.app_id == app_id,
                                    MetadataInfo.file_name == file_name,
                                    MetadataInfo.user_id == user_id,
                                    MetadataInfo.customer_id == customer_id,
                                    MetadataInfo.timestamp == timestamp))
            ret = filt_ret.delete()
        if ret:
            flag = True
            g, region_id = self.select_addr(sgw_info, lock)[0: 2]
            addr = next(g)
            sgw_ip = proxy_ip = addr[0]
            sgw_port = proxy_port = addr[1]
            sgw_id = proxy_id = addr[2]
            
            fmt_body = Constant.FMT_TASKINFO_FIXED
            body = [operation, region_id, site_id, app_id, timestamp, sgw_port,
                    proxy_port, sgw_ip, proxy_ip, sgw_id, proxy_id, file_len,
                    file_md5, file_name, metadata_len]
            body_pack = struct.pack(fmt_body, *body)
            logger.info("处理Client删除元数据的请求,回复的body为：{}".format(body))
        else:
            flag = False
            body_json = json.dumps(body_unpack)
            body_pack = body_json.encode('utf-8')
        return flag, body_pack    
    
    def handle_remote_del(self, head_unpack, body, conn, sgw_info, lock):
        """
        @:在本地删除成功后将删除成功与否的信息发给发起请求的MetadataServer
        """
#         (total_size, major, minor, src_type, dst_type, meta_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_remote_del,处理远端MetadataServer删除元数据的请求')
        total_size = head_unpack[0]
        meta_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        ack_code = head_unpack[10]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
        
        fmt_body = Constant.FMT_TASKINFO_FIXED
        body_unpack = struct.unpack(fmt_body,
                                 body[: Constant.TASKINFO_FIXED_LENGTH])
        metadata_pack = body[Constant.TASKINFO_FIXED_LENGTH:]
        metadata_unpack = json.loads(metadata_pack.decode('utf-8'))
        
        dst_type = Constant.METADATA_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = meta_src_id
        command = Constant.REMOTE_DEL_RESP
        
        flag, body_pack = self.handle_local_del(body_unpack, metadata_unpack,
                                               sgw_info, lock)
        if flag:
            ack_code = Constant.ACK_REMOTE_DEL_SUCCESS
        else:
            ack_code = Constant.ACK_REMOTE_DEL_FAILED
        
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code,
                  total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        logger.info("处理远端MetadataServer删除元数据的请求,回复的head为：{}".format(header))
        data = head_pack + body_pack + metadata_pack
        conn.sendall(data)
        
    def handle_config_upgrade(self, head_unpack, body, conn, queue_recv,
                              conf_info):
        """
        @:处理Client配置升级
        @:Client更新配置请求的消息体json格式：
        {"site_id": site_id}
        @:MetadataServer回复的消息体json格式：
        {"region_id": region_id,
        "metadata_ip": metadata_ip,
        "metadata_port": metadata_port}
        """
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_config_upgrade，处理Client配置升级的请求')
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        offset = head_unpack[12]
        
        body_unpack = json.loads(body.decode('utf-8'))
        site_id = body_unpack.get('site_id') 
        
        # 生成消息体 
        query_body_unpack = self.get_conf(site_id, queue_recv, conf_info)
        logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
        region_id_info = query_body_unpack.get('region_id')
        region_id = region_id_info[0]
        metadata_ip = region_id_info[1]
        metadata_port = region_id_info[2]

        # test
#         region_id = 1
#         metadata_ip = '192.168.0.100'
#         metadata_port = 3434
        body = {}
        body['region_id'] = region_id
        body['metadata_ip'] = metadata_ip
        body['metadata_port'] = metadata_port
        body_json = json.dumps(body)
        body_pack = body_json.encode('utf-8')
        
        total = count = len(body_pack)
        total_size = Constant.HEAD_LENGTH + total
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        command = Constant.CLIENT_CONFIG_UPGRADE_RESP
        ack_code = Constant.ACK_CLIENT_CONFIG_UPGRADE
        
        # 生成头部
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, src_id, dst_id, trans_id, sequence, command,
                  ack_code, total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        logger.info("处理Client配置升级的请求,回复的head为：{}".format(header))
        logger.info("处理Client配置升级的请求,回复的body为：{}".format(body_json))
        data = head_pack + body_pack
        conn.sendall(data)
    
    def handle_client_upgrade(self, head_unpack, body, conn, queue_recv, conf_info):
        """
        @:处理Client软件升级
        @:Client软件升级请求的消息体json格式：
        {"site_id": site_id}
        @:MetadataServer回复的消息体json格式：
        {"file_url": file_url,
        "file_name": file_name,
        "file_md5": file_md5,
        "file_len": file_len,        # 暂为空
        "file_path": file_path}      # 暂为空
        """
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info('执行handle_client_upgrade，处理Client软件升级的请求')
        client_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        offset = head_unpack[12]
        
        body_unpack = json.loads(body.decode('utf-8'))
        site_id = body_unpack.get('site_id')
         
        # 生成消息体 
        query_body_unpack = self.get_conf(site_id, queue_recv, conf_info)
        logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
        file_url = query_body_unpack.get('file_url')
        file_name = query_body_unpack.get('file_name')
        file_md5 = query_body_unpack.get('file_md5')
        
        #test
#         file_url = 'http://www.baidu.com'
#         file_name = 'helloworld'
#         file_md5 = 0
        body = {}
        body['file_url'] = file_url
        body['file_name'] = file_name
        body['file_md5'] = file_md5
        body_json = json.dumps(body)
        body_pack = body_json.encode('utf-8')
        
        total = count = len(body_pack)
        total_size = Constant.HEAD_LENGTH + total
        src_id = int(Config.src_id, 16)
        dst_id = client_src_id
        command = Constant.CLIENT_UPGRADE_RESP
        ack_code = Constant.ACK_CLIENT_UPGRADE
        
        # 生成头部
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type,
                  self.dst_type, src_id, dst_id, trans_id, sequence, command,
                  ack_code, total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        logger.info("处理Client软件升级的请求,回复的head为：{}".format(header))
        logger.info("处理Client软件升级的请求,回复的body为：{}".format(body_json))
        data = head_pack + body_pack
        conn.sendall(data)
        
