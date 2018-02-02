#!/usr/bin/python3
# -*- coding=utf-8 -*-
import multiprocessing
import selectors
import socket

from config import Config, Constant
from log import logger

try:
    workers = int(Config._workers)
except ValueError:
    logger.error('配置文件meta.ini中的worker参数配置为非数字，请检查配置文件。')
else:
    if workers <= 0 or workers > Constant.DEFAULT_WORKERS:
        workers = Constant.DEFAULT_WORKERS
 
HOST = Config.listening_ip
try:
    PORT = int(Config.listening_port)
except ValueError:
    logger.error('配置文件meta.ini中的listening【section】port参数配置为非数字，请检查配置文件。')
ADDR = (HOST, PORT)
  
sel = selectors.DefaultSelector()    

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(ADDR)

def listening():
    sock.listen(10)
    sock.setblocking(False)
    sel.register(sock, selectors.EVENT_READ, accept)
    
    while True:
        events = sel.select()
        for key, mask in events:
            callback = key.data
            callback(key.fileobj, mask)
        
def accept(sock, mask):
    while True:
        conn, addr = sock.accept()
        logger.info("accepted connection from {}".format(addr))
        conn.setblocking(False)
        sel.register(conn, selectors.EVENT_READ, read)

def read(conn, mask):
    data = conn.recv(Constant.BUFSIZE)
    if not data:
        sel.unregister(conn)

if __name__ == '__main__':
    for i in range(workers):
        p = multiprocessing.Process(target=listening)
        p.start()
        



