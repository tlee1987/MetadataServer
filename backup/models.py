'''Created on 2017年12月26日@author: litian'''
from sqlalchemy import (Column, Table, MetaData, String, SmallInteger, Boolean,
                        Integer, BigInteger, create_engine)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from contextlib import contextmanager

from config import Config

db_conn_str = Config.db_conn_str
engine = create_engine(db_conn_str)
metadata = MetaData(engine)
 
client_status = Table('client_status', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('site_id', Integer),
                      Column('client_disk_total', BigInteger),
                      Column('client_disk_free', BigInteger),
                      Column('config_version', SmallInteger),
                      Column('client_version', SmallInteger),
                      Column('transactions_num', Integer),
                      Column('timestamp', Integer))
 
sgw_status = Table('sgw_status', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('region_id', SmallInteger),
                   Column('system_id', Integer),
                   Column('group_id', Integer),
                   Column('sgw_version', SmallInteger),
                   Column('cpu_percent', Integer),
                   Column('mem_used', BigInteger),
                   Column('mem_free', BigInteger),
                   Column('disk_used', BigInteger),
                   Column('disk_free', BigInteger),
                   Column('netio_input', BigInteger),
                   Column('netio_output', BigInteger),
                   Column('conn_state', BigInteger),
                   Column('conn_dealed', BigInteger),
                   Column('timestamp', Integer))

metadata_status = Table('metadata_status', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('region_id', SmallInteger),
                        Column('system_id', Integer),
                        Column('meta_version', SmallInteger),
                        Column('cpu_percent', Integer),
                        Column('mem_used', BigInteger),
                        Column('mem_free', BigInteger),
                        Column('disk_used', BigInteger),
                        Column('dis_free', BigInteger),
                        Column('netio_input', BigInteger),
                        Column('netio_output', BigInteger),
                        Column('timestamp', Integer))

metadata_info = Table('metadata_info', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('site_id', Integer),
                 Column('app_id', Integer),
                 Column('file_name', String(512)),
                 Column('region_id', SmallInteger),
                 Column('system_id', Integer),
                 Column('group_id', Integer),
                 Column('user_id', Integer),
                 Column('customer_id', String(32)),
                 Column('timestamp', Integer))
 
status_flag = Table('status_flag', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('connected', Boolean),
                    Column('disconnected', Boolean))
 
metadata.create_all(engine)

Base = declarative_base()

class ClientStatus(Base):
    __tablename__ = 'client_status'
    
    id = Column(Integer, primary_key=True)
    site_id = Column(Integer)
    client_disk_total = Column(BigInteger)
    client_disk_free = Column(BigInteger)
    config_version = Column(SmallInteger)
    client_version = Column(SmallInteger)
    transactions_num = Column(Integer)
    timestamp = Column(Integer)
    status_flag = Column(Boolean)


class SgwStaus(Base):
    __tablename__ = 'sgw_status'
    
    id = Column(Integer, primary_key=True)
    region_id = Column(SmallInteger)
    system_id = Column(Integer)
    group_id = Column(Integer)
    sgw_version = Column(SmallInteger)
    cpu_percent = Column(Integer)
    mem_used = Column(BigInteger)
    mem_free = Column(BigInteger)
    disk_used = Column(BigInteger)
    disk_free = Column(BigInteger) 
    netio_input = Column(BigInteger)
    netio_output = Column(BigInteger)
    conn_state = Column(BigInteger)
    conn_dealed = Column(BigInteger)
    timestamp = Column(Integer)
    status_flag = Column(Boolean)


class MetadataStatus(Base):
    __tablename__ = 'metadata_status'
    
    id = Column(Integer, primary_key=True)
    region_id = Column(SmallInteger)
    system_id = Column(Integer)
    meta_version = Column(SmallInteger)
    cpu_percent = Column(Integer)
    mem_used = Column(BigInteger)
    mem_free = Column(BigInteger)
    disk_used = Column(BigInteger)
    disk_free = Column(BigInteger)
    netio_input = Column(BigInteger)
    netio_output = Column(BigInteger)
    timestamp = Column(Integer)
    

class MetadataInfo(Base):
    __tablename__ = 'metadata_info'
    
    id = Column(Integer, primary_key=True)
    site_id = Column(Integer)
    app_id = Column(Integer)
    file_name = Column(String(512))
    region_id = Column(SmallInteger)
    system_id = Column(Integer)
    group_id = Column(Integer)
    user_id = Column(Integer)
    customer_id = Column(String(32))
    timestamp = Column(Integer)
    
    
class StatusFlag(Base):
    __tablename__ = 'status_flag'
    
    id = Column(Integer, primary_key=True)
    connected = Column(Boolean)
    disconnected = Column(Boolean)
   
    
SessionType = scoped_session(sessionmaker(bind=engine))  

@contextmanager
def session_scope():  
    session = SessionType()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


    
    
    
    
    
    

















