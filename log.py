#!/usr/bin/python3
# -*- coding=utf-8 -*-
import os
import logging.config

from config import basedir

logdir = os.path.join(basedir, 'logs')
logini_path = os.path.join(basedir, 'log.ini')

if not os.path.exists(logdir):
    os.mkdir(logdir)
    
# logger = logging.getLogger('metadataserver')
# def initLog():
#     logger.setLevel(logging.DEBUG)
#     
#     fh = logging.FileHandler('./logs/metadataserver.log')
#     fh.setLevel(logging.DEBUG)
#     ch = logging.StreamHandler()
#     ch.setLevel(logging.DEBUG)
#     
#     formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
#     fh.setFormatter(formatter)
#     ch.setFormatter(formatter)
#     
#     logger.addHandler(fh)
#     logger.addHandler(ch)
#     
# initLog()
logging.config.fileConfig(logini_path)
logger = logging.getLogger('metadataserver')
