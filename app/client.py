#!/usr/bin/python3
# -*- coding=utf-8 -*-
import struct
import json
import time
import socket
# from itertools import cycle
from operator import itemgetter
from sqlalchemy import and_, desc, asc

from log import logger
from config import Config, Constant
from app.configserver import config_server
from app.models import ClientStatus, MetadataInfo, session_scope


class Client:
    
    def __init__(self):
        self.major = Constant.MAJOR_VERSION
        self.minor = Constant.MINOR_VERSION
        self.src_type = Constant.METADATA_TYPE
        self.dst_type = Constant.CLIENT_TYPE
        
    def _generate_resp_hb(self, head_unpack, config_version, client_version):
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
    
    def get_conf(self, site_id, conf_info):
        '''
        @:向ConfigServer查询Client的配置信息
        @:查询不成功查3次
        '''
        logger.info('执行get_conf,向ConfigServer发送查询信息')
        for i in range(Constant.try_times):
            logger.info('第{}次发送查询信息'.format(i+1))
            config_server.send_msg(site_id)
            key = str(site_id)
            body_unpack = conf_info.get(key)
            logger.info("get_conf中的body_unpack:{}".format(body_unpack))
            if body_unpack:
                conf_info.pop(key)
                return body_unpack
            else:
                logger.info('延时1秒发送查询请求')
                time.sleep(3)
        else:
            logger.error('查询3次未查到site_id:{}的配置信息'.format(site_id))
        
    def handle_hb(self, head_unpack, body, conn, sel, conf_info, version_info):
        """
        @:处理Client心跳消息
        @:心跳消息内容字段：
        site_id    client_disk_total    client_disk_free    config_version
        client_version    transactions_num    timestamp
        """
        logger.info('执行handle_hb，处理Client心跳消息')
        try:
            fmt_body = '!IQQHHII'
            body_unpack = struct.unpack(fmt_body, body)
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
#             sql_queue.put_nowait(client_status)    
            with session_scope() as session:
                session.add(client_status)
            
            if str(site_id) not in version_info:
                conf_body_unpack = self.get_conf(site_id, conf_info)
                if conf_body_unpack:
                    config_version = conf_body_unpack.get('config_version')
                    client_version = conf_body_unpack.get('client_version')
                    version_info[str(site_id)] = [config_version, client_version]
                else:
                    logger.error('经过{}次查询未查询到配置信息'.format(Constant.try_times))
                    config_version = 0
                    client_version = 0
            else:
                version_list = version_info.get(str(site_id))
                config_version = version_list[0]
                client_version = version_list[1]
            
            response = self._generate_resp_hb(head_unpack, config_version,
                                              client_version)
            try:
                conn.sendall(response)
            except socket.error:
                sel.unregister(conn)
                conn.close()
    
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
            addr_temp = sgw_disk_temp[max_disk_free][0]
            addr = addr_temp.popleft()
            addr_temp.append(addr)
            region_id = sgw_disk_temp[max_disk_free][1]
            system_id = sgw_disk_temp[max_disk_free][2]
            group_id = sgw_disk_temp[max_disk_free][3]
            return addr, region_id, system_id, group_id
        finally:
            lock.release()
        
    def handle_upload(self, head_unpack, body, conn, sel, sgw_info, lock):
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
            
        addr, region_id, system_id, group_id = self.select_addr(sgw_info, lock)
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
        try:
            conn.sendall(data)
        except socket.error:
            sel.unregister(conn)
            conn.close()
        
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
#         sql_queue.put_nowait(metadata_info)
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
#             sql_queue.put_nowait(metadata_sql)
            with session_scope() as session:
                session.add(metadata_sql)
            
            metadata_info.pop(file_md5)
            
    def handle_query_num(self, head_unpack, body, conn, sel, conf_info):
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
        query_body_unpack = self.get_conf(site_id, conf_info)
        logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
        remote_res = 0
        if query_body_unpack:
            site_region_id = query_body_unpack.get(str(site_id))
            if site_region_id:
                for region_id in site_region_id:
                    remote_metadata_info = query_body_unpack.get(region_id)
                    logger.info("remote_metadata_info"
                                ":{}".format(remote_metadata_info))
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
        try:
            conn.sendall(data)
        except socket.error:
            sel.unregister(conn)
            conn.close()
        
    def handle_query_data(self, head_unpack, body, conn, sgw_info, lock,
                          conf_info):
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
        
        query_body_unpack = self.get_conf(site_id, conf_info)
        logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
        if query_body_unpack:
            site_region_id = query_body_unpack.get(str(site_id))
            if site_region_id:
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

        addr = self.select_addr(sgw_info, lock)[0]
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
            
    def handle_query_data1(self, head_unpack, body, conn, sgw_info, lock,
                          conf_info):
        """
        @:查询元数据信息
        """
#         (total_size, major, minor, src_type, dst_type, client_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        client_src_id = head_unpack[5]
        offset = head_unpack[12]
        count = head_unpack[13]
        site_id = client_src_id
        
        body_unpack = json.loads(body.decode('utf-8'))
        order_by = body_unpack.get('order_by')
        desc_key = body_unpack.get('desc')
         
        query_body_unpack = self.get_conf(site_id, conf_info)
        logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
        
        n = int(offset/count) + 1
        if query_body_unpack:
            site_region_id = query_body_unpack.get(str(site_id))
            if site_region_id:
                if n <= 10:
                    self.handle_query_data(head_unpack, body, conn, sgw_info,
                                           lock, conf_info)
                else:    
                    offset_local = 0
                    local_ret = self._get_local_query_data1(body, sgw_info, lock,
                                                            offset_local, count)
                    local_remote_dict = {}
                    local_remote_dict['region_id'] = local_ret
                    sock_dict = {}
                    for region_id in site_region_id:
                        remote_metadata_info = query_body_unpack.get(region_id)
                        meta_ip = str(remote_metadata_info[1])
                        meta_port = int(remote_metadata_info[2])
                        meta_src_id = remote_metadata_info[3]
                        addr = (meta_ip, meta_port)
                        sock = self._generate_meta_sock(addr)
                        sock_dict[region_id] = sock
                        offset_remote = 0
                        self._send_query_data1(head_unpack, body, sock, meta_src_id,
                                              offset_remote, count)
                        meta_body = self.recv_remote_msg(sock)[1]
                        meta_body_unpack = json.loads(meta_body.decode('utf-8'))
                        remote_ret = meta_body_unpack.get('site_id')
                        local_remote_dict[region_id] = remote_ret
                        local_ret.extend(remote_ret)
                    local_tmp = sorted(local_ret, key=itemgetter(*order_by),
                                       reverse=desc_key)
                    ret_tmp = local_tmp[: count]
                    ret_tmp_set = set(ret_tmp)
                    
                    local_region_id = local_remote_dict.get('region_id')
                    local_region_id_set = set(local_region_id)
                    local_intersection_set = local_region_id_set.intersection(ret_tmp_set)
                    local_intersection_list = list(local_intersection_set)
                    local_num = len(local_intersection_list)
                    for local_item in local_intersection_list:
                        local_tmp.remove(local_item)
                        local_region_id.remove(local_item)
                    offset_local = offset_local + count
                    local_ret1 = self._get_local_query_data1(body, sgw_info, lock,
                                                            offset_local, local_num)    
                    local_tmp.extend(local_ret1)
                    local_region_id.extend(local_ret1)
                    local_remote_dict['region_id'] = local_region_id
                    
                    for region_id in site_region_id:
                        remote_region_id = local_remote_dict.get(region_id)
                        remote_region_id_set = set(remote_region_id)
                        remote_intersection_set = remote_region_id_set.intersection(ret_tmp_set)
                        remote_intersection_list = list(remote_intersection_set)
                        remote_num = len(remote_intersection_list)
                        for remote_item in remote_intersection_list:
                            local_tmp.remove(remote_item)
                            remote_region_id.remove(remote_item)
                            
                        remote_metadata_info = query_body_unpack.get(region_id)
                        meta_src_id = remote_metadata_info[3]   
                        sock = sock_dict.get(region_id)
                        
                        offset_remote = offset_remote + count
                        self._send_query_data1(head_unpack, body, sock, meta_src_id,
                                              offset_remote, remote_num)
                        meta_body = self.recv_remote_msg(sock)[1]
                        meta_body_unpack = json.loads(meta_body.decode('utf-8'))
                        remote_ret1 = meta_body_unpack.get('site_id')
                        local_tmp.extend(remote_ret1)
                        remote_region_id.extend(remote_ret1)
                        local_remote_dict[region_id] = remote_ret1
                    
                    local_tmp.sort(key=itemgetter(*order_by), reverse=desc_key)
                    ret_tmp1 = local_tmp[: count]
                    ret_tmp_set1 = set(ret_tmp1)
                    
                    for _ in range(n-1):
                        local_region_id1 = local_remote_dict.get('region_id')
                        local_region_id_set1 = set(local_region_id1)
                        local_intersection_set1 = local_region_id_set1.intersection(ret_tmp_set1)
                        local_intersection_list1 = list(local_intersection_set1)
                        local_num1 = len(local_intersection_list1)
                        for local_item1 in local_intersection_list1:
                            local_tmp.remove(local_item1)
                            local_region_id1.remove(local_item1)
                        offset_local = offset_local + local_num1
                        local_ret2 = self._get_local_query_data1(body, sgw_info, lock,
                                                                offset_local, local_num1)    
                        local_tmp.extend(local_ret2)
                        local_region_id1.extend(local_ret2)
                        local_remote_dict['region_id'] = local_region_id1
                        
                        for region_id in site_region_id:
                            remote_region_id1 = local_remote_dict.get(region_id)
                            remote_region_id_set1 = set(remote_region_id1)
                            remote_intersection_set1 = remote_region_id_set1.intersection(ret_tmp_set1)
                            remote_intersection_list1 = list(remote_intersection_set1)
                            remote_num1 = len(remote_intersection_list1)
                            for remote_item1 in remote_intersection_list1:
                                local_tmp.remove(remote_item1)
                                remote_region_id1.remove(remote_item1)
                                
                            remote_metadata_info = query_body_unpack.get(region_id)
                            meta_src_id = remote_metadata_info[3]   
                            sock = sock_dict.get(region_id)
                            
                            offset_remote = offset_remote + remote_num1
                            self._send_query_data1(head_unpack, body, sock, meta_src_id,
                                                  offset_remote, remote_num1)
                            meta_body = self.recv_remote_msg(sock)[1]
                            meta_body_unpack = json.loads(meta_body.decode('utf-8'))
                            remote_ret2 = meta_body_unpack.get('site_id')
                            local_tmp.extend(remote_ret2)
                            remote_region_id1.extend(remote_ret2)
                            local_remote_dict[region_id] = remote_ret2
                        
                        local_tmp.sort(key=itemgetter(*order_by), reverse=desc_key)
                        ret_tmp1 = local_tmp[: count]
                        ret_tmp_set1 = set(ret_tmp1)
                        
                    local_tmp.sort(key=itemgetter(*order_by), reverse=desc_key)
                    rets = local_tmp[: count]
                    self.proxy_query_data(head_unpack, rets, conn, sgw_info, lock)
            else:
                self.handle_local_query_data(head_unpack, body, conn, sgw_info, lock)
        else:
            self.handle_local_query_data(head_unpack, body, conn, sgw_info, lock)
            
    def _get_local_query_data1(self, body, sgw_info, lock, offset, count):
        """
        :MetadataServer收到查询请求后进行本地查询
        """
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
         
        addr = self.select_addr(sgw_info, lock)[0]
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
             
            for metadata_info in ret[offset: offset + count]:
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
    
    def _send_query_data1(self, head_unpack, body, sock, meta_src_id,
                          offset_remote, count_remote):
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
        offset = offset_remote
        count = count_remote
         
        dst_type = Constant.METADATA_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = int(meta_src_id, 16)
        command = Constant.REMOTE_QUERY_DATA
        
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code,
                  total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        data = head_pack + body
        try:
            sock.sendall(data)
        except socket.error:
            sock.close()
        
    def _generate_meta_sock(self, addr):
        """
        @:创建与RemoteMetadataServer通信的socket
        """
        logger.info("执行_generate_meta_sock，生成与RemoteMetadataServer通信的socket")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(addr)
        except socket.error:
            logger.error('该请求的地址{}无效，与RemoteMetadataServer连接失败'.format(addr))
            sock.close()
        return sock
        
    def _generate_query_num(self, head_unpack, body, meta_src_id):
        """
        @:生成向RemoteMetadataServer查询记录数量的请求消息
        """
#         (total_size, major, minor, src_type, dst_type, src_id, client_dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        logger.info("执行_generate_query_num，生成向RemoteMetadataServer"
                    "查询记录数量的请求消息")
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
        logger.info("生成向RemoteMetadataServer查询记录数量的请求消息的"
                    "head：{}".format(header))
        logger.info("生成向RemoteMetadataServer查询记录数量的请求消息的"
                    "body:{}".format(body))
        data = head_pack + body
        return data
    
    def _send_query_num(self, head_unpack, body, sock, meta_src_id):
        """
        @:向RemoteMetadataServer发送查询消息
        """
        logger.info("执行_send_query_num，向RemoteMetadataServer发送查询消息")
        data = self._generate_query_num(head_unpack, body, meta_src_id)
        try:
            sock.sendall(data)
        except socket.error:
            sock.close()
        
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
        
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, self.major, self.minor, self.src_type, dst_type,
                  src_id, dst_id, trans_id, sequence, command, ack_code,
                  total, offset, count]
        head_pack = struct.pack(fmt_head, *header)
        data = head_pack + body
        try:
            sock.sendall(data)
        except socket.error:
            sock.close()
        
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
            try:
                block = sock.recv(length)
            except socket.error:
                sock.close()
                break
            else:
                length -= len(block)
                blocks.append(block)
        return b''.join(blocks)
     
    def recv_remote_msg(self, sock):
        """
        @:解析RemoteMetadataServer发回的消息
        """
        head_unpack = None
        body = None
        header = self.recvall_remote(sock, Constant.HEAD_LENGTH)
        fmt_head = Constant.FMT_COMMON_HEAD
        try:
            head_unpack = struct.unpack(fmt_head, header)
        except struct.error:
            pass
        else:
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
    
    def _parse_remote_del_msg(self, remote_head_unpack):
        """
        @:解析RemoteMetadataServer发送的删除消息
        """
        ack_code = remote_head_unpack[10]
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
        logger.info("执行_get_local_query_num，获取本地查询到的记录数量")
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
        
        addr = self.select_addr(sgw_info, lock)[0]
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
         
        addr = self.select_addr(sgw_info, lock)[0]
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
        
        addr = self.select_addr(sgw_info, lock)[0]
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
#         addr = self.select_addr(sgw_info, lock)[0]
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
            
    def handle_delete(self, head_unpack, body, conn, sgw_info, lock, conf_info):
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
            query_body_unpack = self.get_conf(site_id, conf_info)
            logger.info("向ConfigServer查询到的信息为：{}".format(query_body_unpack))
            site_region_id = query_body_unpack.get(str(site_id))
            if site_region_id:
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
                    remote_head_unpack, remote_body = self.recv_remote_msg(sock) 
                    flag_remote = self._parse_remote_del_msg(remote_head_unpack)
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
        logger.info('执行proxy_del，代理删除')
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
        
        addr, region_id = self.select_addr(sgw_info, lock)[0: 2]
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
        logger.info('执行_generate_del_msg，生成向RemoteMetadataServer发起删除'
                    '记录的请求消息')
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
        logger.info("生成向RemoteMetadataServer发起删除请求消息的"
                    "head：{}".format(header))
        logger.info("生成向RemoteMetadataServer发起删除请求消息的"
                    "body:{}".format(body))
        data = head_pack + body
        return data        
    
    def _send_del_msg(self, head_unpack, body, sock, meta_src_id):
        """
        @:向RemoteMetadataServer发送删除消息
        """
        logger.info('执行_send_del_msg，向RemoteMetadataServer发送删除消息')
        data = self._generate_del_msg(head_unpack, body, meta_src_id)
        try:
            sock.sendall(data)
        except socket.error:
            sock.close()
        
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
            addr, region_id = self.select_addr(sgw_info, lock)[0: 2]
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
            fmt_body = Constant.FMT_TASKINFO_FIXED
            body_pack = struct.pack(fmt_body, *body_unpack)
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
        
    def handle_config_upgrade(self, head_unpack, body, conn, conf_info):
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
        query_body_unpack = self.get_conf(site_id, conf_info)
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
    
    def handle_client_upgrade(self, head_unpack, body, conn, conf_info):
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
        query_body_unpack = self.get_conf(site_id, conf_info)
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
        
