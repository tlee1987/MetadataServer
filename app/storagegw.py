#!/usr/bin/python3
# -*- coding=utf-8 -*-
import socket
import struct
import time
from collections import deque

from app.models import SgwStaus, SgwStatic
from app.models import session_scope
from config import Config, Constant
from log import logger


class StorageGW:
    """
    @:sgw心跳消息内容字段:
    region_id    system_id    group_id    sgw_version    listen_ip
    listen_port    timestamp    cpu_percent    mem_total    mem_free
    disk_used    disk_free    netio_input    netio_output
    conn_state    conn_dealed    
    @:保存sgw信息的数据结构
    {group_id1: [disk_free1, [deque[addr1, addr2, ...], region_id, system_id, group_id1]],
    group_id2: [disk_free2, [deque[addr3, addr4, ...], region_id, system_id, group_id2]],
    ...}
    """
    def __init__(self, sgw_id, region_id, system_id, group_id, sgw_version,
                 listen_ip, listen_port, timestamp, cpu_percent, mem_total,
                 mem_free, disk_used, disk_free, conn_state, conn_dealed,
                 netio_input, netio_output):
        self.region_id = region_id
        self.system_id = system_id
        self.group_id = group_id
        self.disk_free = disk_free
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        
        self.sgw_status = SgwStaus(region_id=region_id,
                                   system_id=system_id,
                                   group_id=group_id,
                                   sgw_version=sgw_version,
                                   timestamp=timestamp,
                                   cpu_percent=cpu_percent,
                                   mem_total=mem_total,
                                   mem_free=mem_free,
                                   disk_used=disk_used,
                                   disk_free=disk_free,
                                   conn_state=conn_state,
                                   conn_dealed=conn_dealed,
                                   netio_input=netio_input,
                                   netio_output=netio_output,
                                   sgw_id=sgw_id)
    
    def _generate_resp_hb(self, head_unpack):
        """
        (total_size, major, minor, src_type, dst_type, src_id, dst_id, trans_id,
         sequence, command, ack_code, total, offset, count) = head_unpack
        """
#         (total_size, major, minor, src_type, dst_type, sgw_src_id, dst_id,
#          trans_id, sequence, command, ack_code, total, offset, count) = head_unpack
        sgw_src_id = head_unpack[5]
        trans_id = head_unpack[7]
        sequence = head_unpack[8]
        total = head_unpack[11]
        offset = head_unpack[12]
        count = head_unpack[13]
         
        total_size = Constant.HEAD_LENGTH
        major = Constant.MAJOR_VERSION
        minor = Constant.MINOR_VERSION
        src_type = Constant.METADATA_TYPE
        dst_type = Constant.SGW_TYPE
        src_id = int(Config.src_id, 16)
        dst_id = sgw_src_id
        command = Constant.SGW_HB_RESP
        ack_code = Constant.ACK_SGW_HB
        
        # 构造响应消息头
        fmt_head = Constant.FMT_COMMON_HEAD
        header = [total_size, major, minor, src_type, dst_type, src_id, dst_id,
                  trans_id, sequence, command, ack_code, total, offset, count]
        logger.info("回复sgw心跳消息的head：{}".format(header))
        head_pack = struct.pack(fmt_head, *header)
        return head_pack
    
    def handle_hb(self, head_unpack, conn, sel, sgw_info, lock, sgw_id_info,
                  addr_list):
        """
        @:处理sgw心跳消息
        """
        logger.info('执行handle_hb，处理sgw心跳消息')
        if (self.region_id == Config.region_id and
                                self.system_id == Config.system_id):
            response = self._generate_resp_hb(head_unpack)
            try:
                conn.sendall(response)
            except socket.error:
                conn.close()
                sel.unregister(conn)
            else:
                self.register_sgw(head_unpack, conn, sgw_info, lock,
                                  sgw_id_info, addr_list)
                
                with session_scope() as session:
                    session.add(self.sgw_status)
        else:
            conn.close()
            sel.unregister(conn)
            logger.error('sgw心跳消息中的region_id或者system_id与Metadata Server'
                         '本地配置的region_id和system_id不一致')
            
    def register_sgw(self, head_unpack, conn, sgw_info, lock, sgw_id_info,
                     addr_list):
        """
        @:网关注册
        {group_id1: [disk_free, [[addr1, addr2, ...], region_id, system_id, group_id1]],
        group_id2: [disk_free, [[addr3, addr4, ...], region_id, system_id, group_id2]],
        ...}
        addr = [ip, port, sgw_id]
        sgw_ip为ip地址字符串形式，self.listen_ip为ip地址十进制形式
        """
        logger.info('执行register_sgw,注册sgw网关信息')
        sgw_id = head_unpack[5]
        addr = conn.getpeername()
        sgw_ip = addr[0]
#         ip_bin = inet_aton(addr[0])   
#         ip_hex = hexlify(ip_bin)
#         ip = int(ip_hex.decode('utf-8'), 16)
#         addr_converted = (ip, addr[1], sgw_id)
        addr_converted = (self.listen_ip, self.listen_port, sgw_id)
        
        lock.acquire()
        try:
            if addr_converted not in addr_list:
                addr_list.append(addr_converted)
                logger.info('执行register_sgw后，'
                            'addr_list:{}'.format(addr_list))
                
            if sgw_id not in sgw_id_info:
                sgw_id_info[sgw_id] = int(time.time())
                logger.info('执行register_sgw后，'
                            'sgw_id_info:{}'.format(sgw_id_info))
                sgw_static = SgwStatic(sgw_id=sgw_id,
                                       sgw_ip=sgw_ip,
                                       region_id=self.region_id,
                                       status=True)
                with session_scope() as session:
                    query = session.query(SgwStatic)
                    filt = query.filter(SgwStatic.sgw_id == sgw_id)
                    filt_ret = filt.all()
                    if not filt_ret:
                        session.add(sgw_static)
                    else:
                        filt.update({'status': True})
            else:
                sgw_id_info[sgw_id] = int(time.time())
            
            if self.group_id not in sgw_info:
                sgw_info[self.group_id] = [self.disk_free, [deque(addr_list),
                                           Config.region_id, Config.system_id,
                                           self.group_id]]
                
            elif (self.group_id in sgw_info and
                            self.disk_free < sgw_info[self.group_id][0]):
                sgw_info[self.group_id] = [self.disk_free, [deque(addr_list),
                                           Config.region_id, Config.system_id,
                                           self.group_id]]
                 
            else:
                sgw_info[self.group_id] = [self.disk_free, [deque(addr_list),
                                           Config.region_id, Config.system_id,
                                           self.group_id]]
        finally:
            lock.release()
            
    @staticmethod
    def check_sgw_hb(sgw_id_info, addr_list):
        """
        """
        while True:
            for sgw_id, timestamp in sgw_id_info.items():
                delay = int(time.time()) - timestamp
                if delay > Constant.DELAYED:
                    sgw_id_info.pop(sgw_id)
                    for addr in addr_list:
                        if addr[2] == sgw_id:
                            addr_list.remove(addr)
                    with session_scope() as session:
                        query = session.query(SgwStatic)
                        filt_ret = query.filter(SgwStatic.sgw_id == sgw_id)
                        filt_ret.update({'status': False})
            time.sleep(5)          
        
        
